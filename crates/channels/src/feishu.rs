use futures::{SinkExt, StreamExt};
use blockcell_core::{Config, Error, InboundMessage, Result, truncate_str};
use prost::Message as _;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use tracing::{debug, error, info, warn};

/// Feishu WebSocket Protobuf frame (matches pbbp2.proto)
#[derive(Clone, prost::Message)]
struct Frame {
    #[prost(uint64, tag = "1")]
    seq_id: u64,
    #[prost(uint64, tag = "2")]
    log_id: u64,
    #[prost(uint32, tag = "3")]
    service: u32,
    #[prost(uint32, tag = "4")]
    method: u32,
    #[prost(message, repeated, tag = "5")]
    headers: Vec<FrameHeader>,
    #[prost(string, tag = "6")]
    payload_encoding: String,
    #[prost(string, tag = "7")]
    payload_type: String,
    #[prost(bytes = "vec", tag = "8")]
    payload: Vec<u8>,
    #[prost(string, tag = "9")]
    log_id_new: String,
}

#[derive(Clone, prost::Message)]
struct FrameHeader {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(string, tag = "2")]
    value: String,
}

/// method=0 → Control frame, method=1 → Data frame
const FRAME_METHOD_CONTROL: u32 = 0;
const FRAME_METHOD_DATA: u32 = 1;
/// type header values
const MSG_TYPE_PING: &str = "ping";
const MSG_TYPE_PONG: &str = "pong";
#[allow(dead_code)]
const MSG_TYPE_EVENT: &str = "event";

const FEISHU_OPEN_API: &str = "https://open.feishu.cn/open-apis";
const FEISHU_BASE: &str = "https://open.feishu.cn";
/// Refresh token 5 minutes before expiry.
const TOKEN_REFRESH_MARGIN_SECS: i64 = 300;
/// Feishu API business code for expired/invalid tenant access token.
const FEISHU_INVALID_TOKEN_CODE: i64 = 99_991_663;
/// Minimum interval between streaming card updates (throttle).
const STREAMING_UPDATE_INTERVAL_MS: u64 = 100;

// ---------------------------------------------------------------------------
// Streaming card helpers
// ---------------------------------------------------------------------------

/// Per-card streaming update throttle state.
struct CardStreamState {
    /// Last time a streaming update was successfully sent for this card.
    last_update_time: Option<Instant>,
    /// Text that was skipped due to throttling (sent on next eligible update).
    pending_text: Option<String>,
    /// Sequence counter for CardKit update ordering.
    sequence: u64,
    /// When this card was created (for stale draft cleanup).
    created_at: Instant,
    /// Whether this card was created with a collapsible_panel for reasoning.
    has_collapsible: bool,
}

impl Default for CardStreamState {
    fn default() -> Self {
        Self {
            last_update_time: None,
            pending_text: None,
            sequence: 0,
            created_at: Instant::now(),
            has_collapsible: false,
        }
    }
}

/// Streaming state across all active cards.
#[derive(Default)]
struct StreamingThrottleState {
    /// Per-card state keyed by card_id.
    cards: HashMap<String, CardStreamState>,
}

/// Encode a card_id and message_id into a draft identifier.
///
/// Format: `card:{card_id}:msg:{message_id}`
fn encode_draft_id(card_id: &str, message_id: &str) -> String {
    format!("card:{card_id}:msg:{message_id}")
}

/// Decode a draft identifier into `(card_id, message_id)`.
fn decode_draft_id(draft_id: &str) -> std::result::Result<(&str, &str), Error> {
    let rest = draft_id
        .strip_prefix("card:")
        .ok_or_else(|| Error::Channel("invalid draft id: missing 'card:' prefix".into()))?;
    let (card_id, msg_part) = rest
        .split_once(":msg:")
        .ok_or_else(|| Error::Channel("invalid draft id: missing ':msg:' separator".into()))?;
    if card_id.is_empty() || msg_part.is_empty() {
        return Err(Error::Channel("invalid draft id: empty card_id or message_id".into()));
    }
    Ok((card_id, msg_part))
}

/// Truncate summary text to at most `max_len` characters.
fn truncate_summary(text: &str, max_len: usize) -> String {
    let clean = text.replace('\n', " ").trim().to_string();
    if clean.chars().count() <= max_len {
        clean
    } else {
        let truncated: String = clean.chars().take(max_len.saturating_sub(3)).collect();
        format!("{truncated}...")
    }
}

/// Extract `code` from a Feishu API response body.
fn extract_response_code(body: &serde_json::Value) -> Option<i64> {
    body.get("code").and_then(|c| c.as_i64())
}

/// Extract `data.message_id` from a Feishu API response body.
fn extract_message_id(body: &serde_json::Value) -> std::result::Result<String, Error> {
    body.pointer("/data/message_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| Error::Channel(format!("missing data.message_id in Feishu response: {body}")))
}

/// Compose the streaming card markdown from reasoning, tool trace, and content.
///
/// - `reasoning`: native model thinking (from ReasoningDelta events)
/// - `tool_trace`: tool execution status lines (✅/❌)
/// - `content`: actual LLM response text
///
/// The "thinking" section combines reasoning + tool_trace as a quoted block.
/// A `---` divider separates thinking from content.
/// Sanitize markdown for Feishu CardKit compatibility.
///
/// Feishu card markdown elements support: **bold**, *italic*, ~~strikethrough~~,
/// `code`, ```code blocks```, [link](url), > quote, ---, ordered/unordered lists.
/// They do NOT reliably render `#`/`##`/`###` headers or `- **key**: value` patterns.
///
/// This function converts unsupported patterns to Feishu-friendly equivalents.
fn sanitize_feishu_markdown(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut in_code_block = false;

    for line in text.lines() {
        if !result.is_empty() {
            result.push('\n');
        }

        // Don't transform inside fenced code blocks.
        if line.trim_start().starts_with("```") {
            in_code_block = !in_code_block;
            result.push_str(line);
            continue;
        }
        if in_code_block {
            result.push_str(line);
            continue;
        }

        // Convert `# Header` → `**Header**` (all levels).
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            let hashes_end = trimmed.find(|c: char| c != '#').unwrap_or(trimmed.len());
            if hashes_end <= 6 {
                let rest = trimmed[hashes_end..].trim();
                if !rest.is_empty() {
                    // Preserve leading whitespace from original line.
                    let indent = &line[..line.len() - trimmed.len()];
                    result.push_str(indent);
                    result.push_str("**");
                    result.push_str(rest);
                    result.push_str("**");
                    continue;
                }
            }
        }

        result.push_str(line);
    }

    result
}

fn compose_streaming_card_full(reasoning: &str, tool_trace: &str, content: &str) -> String {
    // Build the thinking section: reasoning + tool trace, separated by divider.
    let thinking = match (reasoning.is_empty(), tool_trace.is_empty()) {
        (true, true) => String::new(),
        (false, true) => reasoning.to_string(),
        (true, false) => tool_trace.to_string(),
        (false, false) => format!("{reasoning}\n\n---\n\n{tool_trace}"),
    };

    if thinking.is_empty() {
        return sanitize_feishu_markdown(content);
    }

    let quoted = thinking.lines().map(|l| format!("> {l}")).collect::<Vec<_>>().join("\n");

    if content.is_empty() {
        // Still in thinking phase.
        return format!("💭 **思考中...**\n{quoted}");
    }

    // Both present — show thinking as quoted block, then separator, then content.
    let sanitized_content = sanitize_feishu_markdown(content);
    format!("💭 **思考过程**\n{quoted}\n\n---\n\n{sanitized_content}")
}

/// Cached tenant access token with expiry timestamp.
#[derive(Default)]
struct CachedToken {
    token: String,
    expires_at: i64, // Unix timestamp (seconds)
}

impl CachedToken {
    fn is_valid(&self) -> bool {
        !self.token.is_empty()
            && chrono::Utc::now().timestamp() < self.expires_at - TOKEN_REFRESH_MARGIN_SECS
    }
}

/// Process-global token cache for the free `send_message` function.
static GLOBAL_TOKEN_CACHE: OnceLock<Mutex<CachedToken>> = OnceLock::new();

fn global_token_cache() -> &'static Mutex<CachedToken> {
    GLOBAL_TOKEN_CACHE.get_or_init(|| Mutex::new(CachedToken::default()))
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    code: i32,
    msg: String,
    tenant_access_token: Option<String>,
    #[serde(default)]
    expire: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct WsEndpointResponse {
    code: i32,
    msg: String,
    data: Option<WsEndpointData>,
}

#[derive(Debug, Deserialize)]
struct WsEndpointData {
    #[serde(rename = "URL")]
    url: String,
}

#[derive(Debug, Deserialize)]
struct FeishuEvent {
    #[serde(default)]
    header: Option<EventHeader>,
    #[serde(default)]
    event: Option<EventBody>,
}

#[derive(Debug, Deserialize)]
struct EventHeader {
    event_id: String,
    event_type: String,
}

#[derive(Debug, Deserialize)]
struct EventBody {
    #[serde(default)]
    message: Option<MessageEvent>,
    #[serde(default)]
    sender: Option<SenderInfo>,
}

#[derive(Debug, Deserialize)]
struct MessageEvent {
    message_id: String,
    chat_id: String,
    message_type: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct SenderInfo {
    sender_id: Option<SenderId>,
    sender_type: String,
}

#[derive(Debug, Deserialize)]
struct SenderId {
    open_id: String,
}

#[derive(Debug, Deserialize)]
struct MessageContent {
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ImageContent {
    image_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FileContent {
    file_key: Option<String>,
    #[serde(default)]
    file_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AudioContent {
    file_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VideoContent {
    file_key: Option<String>,
    #[serde(default)]
    file_name: Option<String>,
}

pub struct FeishuChannel {
    config: Config,
    inbound_tx: mpsc::Sender<InboundMessage>,
    client: Client,
    seen_messages: Arc<Mutex<HashSet<String>>>,
    /// Per-instance token cache (shared across reconnects via Arc).
    token_cache: Arc<Mutex<CachedToken>>,
    /// Directory for downloaded media files.
    media_dir: PathBuf,
    /// Whether streaming card feature is enabled.
    streaming_enabled: bool,
    /// Streaming card throttle state.
    streaming_state: Arc<Mutex<StreamingThrottleState>>,
    /// Active streaming drafts: chat_id → draft_id.
    /// Used by the streaming bridge to route token events to the correct card.
    active_drafts: Arc<Mutex<HashMap<String, String>>>,
}

impl FeishuChannel {
    pub fn new(config: Config, inbound_tx: mpsc::Sender<InboundMessage>) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        let media_dir = std::env::var("BLOCKCELL_WORKSPACE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("workspace"))
            .join("media");

        let streaming_enabled = config.channels.feishu.streaming;

        Self {
            config,
            inbound_tx,
            client,
            seen_messages: Arc::new(Mutex::new(HashSet::new())),
            token_cache: Arc::new(Mutex::new(CachedToken::default())),
            media_dir,
            streaming_enabled,
            streaming_state: Arc::new(Mutex::new(StreamingThrottleState::default())),
            active_drafts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn is_allowed(&self, open_id: &str) -> bool {
        let allow_from = &self.config.channels.feishu.allow_from;

        if allow_from.is_empty() {
            return true;
        }

        allow_from.iter().any(|allowed| allowed == open_id)
    }

    async fn get_tenant_access_token(&self) -> Result<String> {
        let mut cache = self.token_cache.lock().await;
        if cache.is_valid() {
            return Ok(cache.token.clone());
        }
        let (token, expires_in) = fetch_tenant_access_token(
            &self.client,
            &self.config.channels.feishu.app_id,
            &self.config.channels.feishu.app_secret,
        )
        .await?;
        cache.token = token.clone();
        cache.expires_at = chrono::Utc::now().timestamp() + expires_in;
        info!(expires_in = expires_in, "Feishu tenant_access_token refreshed");
        Ok(token)
    }

    async fn get_ws_endpoint(&self) -> Result<String> {
        let response = self
            .client
            .post(format!("{}/callback/ws/endpoint", FEISHU_BASE))
            .header("locale", "zh")
            .json(&serde_json::json!({
                "AppID": self.config.channels.feishu.app_id,
                "AppSecret": self.config.channels.feishu.app_secret,
            }))
            .send()
            .await
            .map_err(|e| Error::Channel(format!("Failed to get WS endpoint: {}", e)))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Error::Channel(format!("Failed to read endpoint response body: {}", e)))?;

        if !status.is_success() {
            return Err(Error::Channel(format!(
                "Feishu endpoint HTTP {}: {}",
                status, body
            )));
        }

        let endpoint_resp: WsEndpointResponse = serde_json::from_str(&body)
            .map_err(|e| Error::Channel(format!("Failed to parse endpoint response: {} | body: {}", e, truncate_str(&body, 500))))?;

        if endpoint_resp.code != 0 {
            return Err(Error::Channel(format!(
                "Feishu endpoint error code={} msg={} | body: {}",
                endpoint_resp.code, endpoint_resp.msg, truncate_str(&body, 500)
            )));
        }

        endpoint_resp
            .data
            .map(|d| d.url)
            .ok_or_else(|| Error::Channel(format!("No endpoint URL in response | body: {}", truncate_str(&body, 500))))
    }

    pub async fn run_loop(self: Arc<Self>, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        if !self.config.channels.feishu.enabled {
            info!("Feishu channel disabled");
            return;
        }

        if self.config.channels.feishu.app_id.is_empty() {
            warn!("Feishu app_id not configured");
            return;
        }

        info!("Feishu channel starting");

        loop {
            tokio::select! {
                result = self.connect_and_run() => {
                    match result {
                        Ok(_) => {
                            info!("Feishu connection closed normally");
                        }
                        Err(e) => {
                            error!(error = %e, "Feishu connection error, reconnecting in 5s");
                            tokio::select! {
                                _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
                                _ = shutdown.recv() => {
                                    info!("Feishu channel shutting down");
                                    break;
                                }
                            }
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("Feishu channel shutting down");
                    break;
                }
            }
        }
    }

    async fn connect_and_run(&self) -> Result<()> {
        let ws_url = self.get_ws_endpoint().await?;

        info!(url = %ws_url, "Connecting to Feishu WebSocket");

        let url = url::Url::parse(&ws_url)
            .map_err(|e| Error::Channel(format!("Invalid WebSocket URL: {}", e)))?;

        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| Error::Channel(format!("WebSocket connection failed: {}", e)))?;

        info!("Connected to Feishu WebSocket");

        let (mut write, mut read) = ws_stream.split();

        while let Some(msg) = read.next().await {
            match msg {
                Ok(WsMessage::Binary(data)) => {
                    // Feishu uses Protobuf binary frames
                    match Frame::decode(data.as_slice()) {
                        Ok(frame) => {
                            let msg_type = frame.headers.iter()
                                .find(|h| h.key == "type")
                                .map(|h| h.value.as_str())
                                .unwrap_or("");

                            if frame.method == FRAME_METHOD_CONTROL {
                                if msg_type == MSG_TYPE_PING {
                                    // Respond with pong
                                    let pong = Frame {
                                        seq_id: frame.seq_id,
                                        log_id: frame.log_id,
                                        service: frame.service,
                                        method: FRAME_METHOD_CONTROL,
                                        headers: vec![FrameHeader {
                                            key: "type".to_string(),
                                            value: MSG_TYPE_PONG.to_string(),
                                        }],
                                        payload: frame.payload.clone(),
                                        ..Default::default()
                                    };
                                    let mut buf = Vec::new();
                                    if prost::Message::encode(&pong, &mut buf).is_ok() {
                                        if let Err(e) = write.send(WsMessage::Binary(buf)).await {
                                            error!(error = %e, "Failed to send pong frame");
                                        } else {
                                            debug!("Sent pong to Feishu");
                                        }
                                    }
                                }
                            } else if frame.method == FRAME_METHOD_DATA {
                                // Parse payload as JSON event
                                debug!(method = frame.method, msg_type = %msg_type, payload_len = frame.payload.len(), "Feishu data frame");
                                match std::str::from_utf8(&frame.payload) {
                                    Ok(text) => {
                                        info!(payload = %truncate_str(text, 500), "Feishu raw event payload");
                                        // Send ACK frame
                                        let ack = Frame {
                                            seq_id: frame.seq_id,
                                            log_id: frame.log_id,
                                            service: frame.service,
                                            method: FRAME_METHOD_DATA,
                                            headers: frame.headers.clone(),
                                            payload: b"{\"code\":200}".to_vec(),
                                            ..Default::default()
                                        };
                                        let mut buf = Vec::new();
                                        if prost::Message::encode(&ack, &mut buf).is_ok() {
                                            if let Err(e) = write.send(WsMessage::Binary(buf)).await {
                                                error!(error = %e, "Failed to send ACK");
                                            }
                                        }
                                        if let Err(e) = self.handle_message(text).await {
                                            error!(error = %e, "Failed to handle Feishu event");
                                        }
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Feishu frame payload is not UTF-8");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to decode Feishu Protobuf frame");
                        }
                    }
                }
                Ok(WsMessage::Text(text)) => {
                    // Fallback: some frames may be text JSON
                    if let Err(e) = self.handle_message(&text).await {
                        error!(error = %e, "Failed to handle Feishu text message");
                    }
                }
                Ok(WsMessage::Close(_)) => {
                    info!("Feishu WebSocket closed");
                    break;
                }
                Ok(WsMessage::Ping(data)) => {
                    if let Err(e) = write.send(WsMessage::Pong(data)).await {
                        error!(error = %e, "Failed to send WS pong");
                    }
                }
                Err(e) => {
                    error!(error = %e, "WebSocket error");
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Download a Feishu media resource (image/file/audio/video) to the media dir.
    /// Returns the local file path on success.
    async fn download_media(
        &self,
        message_id: &str,
        file_key: &str,
        file_type: &str,
        file_name: Option<&str>,
    ) -> Result<String> {
        let token = self.get_tenant_access_token().await?;

        // Feishu media download endpoint
        let url = format!(
            "{}/im/v1/messages/{}/resources/{}?type={}",
            FEISHU_OPEN_API, message_id, file_key, file_type
        );

        let resp = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| Error::Channel(format!("Feishu media download failed: {}", e)))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Channel(format!(
                "Feishu media download HTTP {}: {}",
                status, body
            )));
        }

        // Determine file extension from content-type or file_name
        let ext = file_name
            .and_then(|n| n.rsplit('.').next())
            .unwrap_or(match file_type {
                "image" => "jpg",
                "audio" => "opus",
                "video" => "mp4",
                _ => "bin",
            });

        tokio::fs::create_dir_all(&self.media_dir)
            .await
            .map_err(|e| Error::Channel(format!("Failed to create media dir: {}", e)))?;

        let filename = format!("feishu_{}_{}.{}", file_type, &file_key[..8.min(file_key.len())], ext);
        let path = self.media_dir.join(&filename);

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| Error::Channel(format!("Failed to read media bytes: {}", e)))?;

        tokio::fs::write(&path, &bytes)
            .await
            .map_err(|e| Error::Channel(format!("Failed to write media file: {}", e)))?;

        Ok(path.to_string_lossy().to_string())
    }

    async fn handle_message(&self, text: &str) -> Result<()> {
        let event: FeishuEvent = serde_json::from_str(text).map_err(|e| {
            warn!(error = %e, raw = %truncate_str(text, 500), "Failed to parse Feishu event");
            Error::Channel(format!("Failed to parse Feishu event: {}", e))
        })?;

        let header = match event.header {
            Some(h) => h,
            None => return Ok(()),
        };

        // Dedup by event_id
        {
            let mut seen = self.seen_messages.lock().await;
            if seen.contains(&header.event_id) {
                debug!(event_id = %header.event_id, "Duplicate event, skipping");
                return Ok(());
            }
            seen.insert(header.event_id.clone());
            if seen.len() > 1000 {
                let to_remove: Vec<_> = seen.iter().take(100).cloned().collect();
                for id in to_remove { seen.remove(&id); }
            }
        }

        if header.event_type != "im.message.receive_v1" {
            debug!(event_type = %header.event_type, "Ignoring non-message event");
            return Ok(());
        }

        let event_body = match event.event {
            Some(e) => e,
            None => return Ok(()),
        };

        if let Some(sender) = &event_body.sender {
            if sender.sender_type == "bot" {
                debug!("Skipping bot message");
                return Ok(());
            }
        }

        let message = match event_body.message {
            Some(m) => m,
            None => return Ok(()),
        };

        let sender_id = event_body
            .sender
            .and_then(|s| s.sender_id)
            .map(|id| id.open_id)
            .unwrap_or_default();

        if !self.is_allowed(&sender_id) {
            debug!(sender_id = %sender_id, "Sender not in allowlist, ignoring");
            return Ok(());
        }

        let (content_text, media_paths) = match message.message_type.as_str() {
            "text" => {
                let mc: MessageContent = serde_json::from_str(&message.content)
                    .map_err(|e| Error::Channel(format!("Failed to parse text content: {}", e)))?;
                let t = mc.text.unwrap_or_default();
                if t.is_empty() { return Ok(()); }
                (t, vec![])
            }
            "image" => {
                let mc: ImageContent = serde_json::from_str(&message.content)
                    .unwrap_or(ImageContent { image_key: None });
                let key = mc.image_key.unwrap_or_default();
                let mut paths = vec![];
                if !key.is_empty() {
                    match self.download_media(&message.message_id, &key, "image", None).await {
                        Ok(p) => paths.push(p),
                        Err(e) => error!(error = %e, "Failed to download Feishu image"),
                    }
                }
                ("[图片，已下载到本地，可直接查看或用 read_file 读取]".to_string(), paths)
            }
            "file" => {
                let mc: FileContent = serde_json::from_str(&message.content)
                    .unwrap_or(FileContent { file_key: None, file_name: None });
                let key = mc.file_key.unwrap_or_default();
                let name = mc.file_name.as_deref();
                let mut paths = vec![];
                if !key.is_empty() {
                    match self.download_media(&message.message_id, &key, "file", name).await {
                        Ok(p) => paths.push(p),
                        Err(e) => error!(error = %e, "Failed to download Feishu file"),
                    }
                }
                let desc = format!("[文件: {}，已下载到本地，可用 read_file 读取]", name.unwrap_or("unknown"));
                (desc, paths)
            }
            "audio" => {
                let mc: AudioContent = serde_json::from_str(&message.content)
                    .unwrap_or(AudioContent { file_key: None });
                let key = mc.file_key.unwrap_or_default();
                let mut paths = vec![];
                if !key.is_empty() {
                    match self.download_media(&message.message_id, &key, "audio", None).await {
                        Ok(p) => paths.push(p),
                        Err(e) => error!(error = %e, "Failed to download Feishu audio"),
                    }
                }
                ("[语音消息，已下载到本地，请用 audio_transcribe 工具转写后回复]".to_string(), paths)
            }
            "video" | "media" => {
                let mc: VideoContent = serde_json::from_str(&message.content)
                    .unwrap_or(VideoContent { file_key: None, file_name: None });
                let key = mc.file_key.unwrap_or_default();
                let name = mc.file_name.as_deref();
                let mut paths = vec![];
                if !key.is_empty() {
                    match self.download_media(&message.message_id, &key, "video", name).await {
                        Ok(p) => paths.push(p),
                        Err(e) => error!(error = %e, "Failed to download Feishu video"),
                    }
                }
                let desc = if let Some(n) = name {
                    format!("[视频: {}，已下载到本地]", n)
                } else {
                    "[视频，已下载到本地]".to_string()
                };
                (desc, paths)
            }
            other => {
                debug!(message_type = %other, "Feishu: unsupported message type, skipping");
                return Ok(());
            }
        };

        let inbound = InboundMessage {
            channel: "feishu".to_string(),
            sender_id: sender_id.clone(),
            chat_id: message.chat_id.clone(),
            content: content_text,
            media: media_paths,
            metadata: serde_json::json!({
                "message_id": message.message_id,
                "event_id": header.event_id,
                "message_type": message.message_type,
            }),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        };

        // Create a streaming card before forwarding the message to the runtime.
        // This way, when the LLM starts streaming tokens, the card is already
        // in place and the streaming bridge can forward deltas to it.
        match self.send_streaming_start(&message.chat_id).await {
            Ok(Some(draft_id)) => {
                info!(chat_id = %message.chat_id, draft_id = %draft_id, "Feishu: streaming card created");
            }
            Ok(None) => {
                debug!(chat_id = %message.chat_id, "Feishu: streaming not available, using normal send");
            }
            Err(e) => {
                warn!(error = %e, chat_id = %message.chat_id, "Feishu: failed to create streaming card, falling back to normal send");
            }
        }

        self.inbound_tx
            .send(inbound)
            .await
            .map_err(|e| Error::Channel(e.to_string()))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Streaming card (CardKit) support
    // -----------------------------------------------------------------------

    /// Invalidate the cached token (called when API reports an expired token).
    async fn invalidate_token(&self) {
        let mut cache = self.token_cache.lock().await;
        cache.token.clear();
        cache.expires_at = 0;
    }

    /// Send a raw HTTP request and return (status, parsed JSON body).
    async fn send_request_once(
        &self,
        url: &str,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let resp = self
            .client
            .post(url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .json(body)
            .send()
            .await
            .map_err(|e| Error::Channel(format!("Feishu request failed: {e}")))?;
        let status = resp.status();
        let raw = resp.text().await.unwrap_or_default();
        let parsed = serde_json::from_str::<serde_json::Value>(&raw)
            .unwrap_or_else(|_| serde_json::json!({ "raw": raw }));
        Ok((status, parsed))
    }

    /// Check if the response indicates an invalid/expired token.
    fn should_refresh_token(status: reqwest::StatusCode, body: &serde_json::Value) -> bool {
        status == reqwest::StatusCode::UNAUTHORIZED
            || extract_response_code(body) == Some(FEISHU_INVALID_TOKEN_CODE)
    }

    /// Send a message and return the `message_id` from the response.
    async fn send_message_with_id(
        &self,
        chat_id: &str,
        msg_type: &str,
        content: &str,
    ) -> Result<String> {
        let token = self.get_tenant_access_token().await?;
        let url = format!("{}/im/v1/messages?receive_id_type=chat_id", FEISHU_OPEN_API);

        let wire_content = if msg_type == "text" {
            serde_json::json!({ "text": content }).to_string()
        } else {
            content.to_string()
        };

        let body = serde_json::json!({
            "receive_id": chat_id,
            "msg_type": msg_type,
            "content": wire_content,
        });

        let (status, response) = self.send_request_once(&url, &token, &body).await?;

        if Self::should_refresh_token(status, &response) {
            self.invalidate_token().await;
            let new_token = self.get_tenant_access_token().await?;
            let (retry_status, retry_response) =
                self.send_request_once(&url, &new_token, &body).await?;

            if !retry_status.is_success() || extract_response_code(&retry_response).unwrap_or(0) != 0 {
                return Err(Error::Channel(format!(
                    "Feishu send_message_with_id failed after token refresh: status={retry_status}, body={retry_response}"
                )));
            }
            return extract_message_id(&retry_response);
        }

        let code = extract_response_code(&response).unwrap_or(0);
        if !status.is_success() || code != 0 {
            return Err(Error::Channel(format!(
                "Feishu send_message_with_id failed: status={status}, body={response}"
            )));
        }
        extract_message_id(&response)
    }

    /// Create a streaming card entity via CardKit API.
    ///
    /// Returns the `card_id` from the response.
    /// Returns `(card_id, has_collapsible)`.
    async fn cardkit_create_card(&self, token: &str) -> Result<(String, bool)> {
        let url = format!("{}/cardkit/v1/cards", FEISHU_OPEN_API);

        // Try creating a card with collapsible_panel for reasoning.
        // If the API rejects it (e.g. collapsible_panel not supported),
        // fall back to a simple single-element card.
        let card_json_with_panel = serde_json::json!({
            "schema": "2.0",
            "config": {
                "streaming_mode": true,
                "summary": { "content": "[生成中...]" },
                "streaming_config": {
                    "print_frequency_ms": { "default": 50 },
                    "print_step": { "default": 2 }
                }
            },
            "body": {
                "elements": [
                    {
                        "tag": "collapsible_panel",
                        "expanded": false,
                        "background": {
                            "color": "bg-fill-tag-purple"
                        },
                        "header": {
                            "title": {
                                "tag": "plain_text",
                                "content": "💭 思考过程（点击展开）"
                            },
                            "vertical_align": "center"
                        },
                        "vertical_spacing": "8px",
                        "element_id": "thinking_panel",
                        "elements": [{
                            "tag": "markdown",
                            "content": "",
                            "element_id": "thinking_md"
                        }]
                    },
                    {
                        "tag": "markdown",
                        "content": "⏳",
                        "element_id": "content"
                    }
                ]
            }
        });

        let body_with_panel = serde_json::json!({
            "type": "card_json",
            "data": card_json_with_panel.to_string(),
        });

        let result = self.send_request_once(&url, token, &body_with_panel).await;

        match result {
            Ok((_status, response)) => {
                let code = extract_response_code(&response).unwrap_or(0);
                if code == 0 {
                    if let Some(card_id) = response.pointer("/data/card_id").and_then(|v| v.as_str()) {
                        info!("CardKit card created with collapsible_panel");
                        return Ok((card_id.to_string(), true));
                    }
                }
                // collapsible_panel rejected — fall through to simple card
                let msg = response.get("msg").and_then(|v| v.as_str()).unwrap_or("unknown");
                warn!("CardKit create with collapsible_panel failed (code={code}, msg={msg}), falling back to simple card");
            }
            Err(e) => {
                warn!("CardKit create with collapsible_panel request failed: {e}, falling back to simple card");
            }
        }

        // Fallback: simple single-element card (no collapsible panel).
        let card_json_simple = serde_json::json!({
            "schema": "2.0",
            "config": {
                "streaming_mode": true,
                "summary": { "content": "[生成中...]" },
                "streaming_config": {
                    "print_frequency_ms": { "default": 50 },
                    "print_step": { "default": 2 }
                }
            },
            "body": {
                "elements": [{
                    "tag": "markdown",
                    "content": "⏳",
                    "element_id": "content"
                }]
            }
        });

        let body_simple = serde_json::json!({
            "type": "card_json",
            "data": card_json_simple.to_string(),
        });

        let (_status, response) = self.send_request_once(&url, token, &body_simple).await?;

        let code = extract_response_code(&response).unwrap_or(0);
        if code != 0 {
            let msg = response
                .get("msg")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(Error::Channel(format!(
                "CardKit create card failed: code={code}, msg={msg}"
            )));
        }

        response
            .pointer("/data/card_id")
            .and_then(|v| v.as_str())
            .map(|s| (s.to_string(), false))
            .ok_or_else(|| Error::Channel(format!("missing data.card_id in CardKit response: {response}")))
    }

    /// Update the markdown content of a streaming card element.
    async fn cardkit_update_content(
        &self,
        token: &str,
        card_id: &str,
        content: &str,
        sequence: u64,
    ) -> Result<()> {
        let url = format!(
            "{}/cardkit/v1/cards/{}/elements/content/content",
            FEISHU_OPEN_API, card_id,
        );

        let uuid = format!("s_{card_id}_{sequence}");

        let body = serde_json::json!({
            "content": content,
            "sequence": sequence,
            "uuid": uuid,
        });

        let resp = self
            .client
            .put(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Channel(format!("CardKit update content request failed: {e}")))?;
        let _status = resp.status();
        let raw = resp.text().await.unwrap_or_default();
        let parsed = serde_json::from_str::<serde_json::Value>(&raw)
            .unwrap_or_else(|_| serde_json::json!({ "raw": raw }));

        let code = extract_response_code(&parsed).unwrap_or(0);
        if code != 0 {
            let msg = parsed
                .get("msg")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(Error::Channel(format!(
                "CardKit update content failed: code={code}, msg={msg}"
            )));
        }

        Ok(())
    }

    /// Update the content of a specific element in a CardKit card by its element_id.
    /// This is used to update the thinking_md element inside the collapsible panel.
    async fn cardkit_update_element(
        &self,
        token: &str,
        card_id: &str,
        element_id: &str,
        content: &str,
        sequence: u64,
    ) -> Result<()> {
        let url = format!(
            "{}/cardkit/v1/cards/{}/elements/{}/content",
            FEISHU_OPEN_API, card_id, element_id,
        );

        let uuid = format!("e_{card_id}_{element_id}_{sequence}");

        let body = serde_json::json!({
            "content": content,
            "sequence": sequence,
            "uuid": uuid,
        });

        let resp = self
            .client
            .put(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Channel(format!("CardKit update element '{element_id}' request failed: {e}")))?;
        let _status = resp.status();
        let raw = resp.text().await.unwrap_or_default();
        let parsed = serde_json::from_str::<serde_json::Value>(&raw)
            .unwrap_or_else(|_| serde_json::json!({ "raw": raw }));

        let code = extract_response_code(&parsed).unwrap_or(0);
        if code != 0 {
            let msg = parsed
                .get("msg")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(Error::Channel(format!(
                "CardKit update element '{element_id}' failed: code={code}, msg={msg}"
            )));
        }

        Ok(())
    }

    /// Close streaming mode on a CardKit card.
    async fn cardkit_close_streaming(
        &self,
        token: &str,
        card_id: &str,
        summary: &str,
        sequence: u64,
    ) -> Result<()> {
        let url = format!(
            "{}/cardkit/v1/cards/{}/settings",
            FEISHU_OPEN_API, card_id,
        );

        let uuid = format!("c_{card_id}_{sequence}");

        let settings = serde_json::json!({
            "config": {
                "streaming_mode": false,
                "summary": { "content": summary }
            }
        });

        let body = serde_json::json!({
            "settings": settings.to_string(),
            "sequence": sequence,
            "uuid": uuid,
        });

        let resp = self
            .client
            .patch(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Channel(format!("CardKit close streaming request failed: {e}")))?;
        let _status = resp.status();
        let raw = resp.text().await.unwrap_or_default();
        let parsed = serde_json::from_str::<serde_json::Value>(&raw)
            .unwrap_or_else(|_| serde_json::json!({ "raw": raw }));

        let code = extract_response_code(&parsed).unwrap_or(0);
        if code != 0 {
            let msg = parsed
                .get("msg")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(Error::Channel(format!(
                "CardKit close streaming failed: code={code}, msg={msg}"
            )));
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Public streaming API
    // -----------------------------------------------------------------------

    /// Start a streaming card: create a CardKit card, send it as an interactive
    /// message, and return the encoded draft ID.
    ///
    /// Returns `Ok(Some(draft_id))` on success, `Ok(None)` if streaming is not
    /// available (e.g. token failure).
    pub async fn send_streaming_start(&self, chat_id: &str) -> Result<Option<String>> {
        if !self.streaming_enabled {
            return Ok(None);
        }

        let token = self.get_tenant_access_token().await?;

        // Create streaming card (with one token-refresh retry).
        let (card_id, has_collapsible) = match self.cardkit_create_card(&token).await {
            Ok(result) => result,
            Err(first_err) => {
                if first_err.to_string().contains("99991663") {
                    self.invalidate_token().await;
                    let new_token = self.get_tenant_access_token().await?;
                    self.cardkit_create_card(&new_token).await?
                } else {
                    return Err(first_err);
                }
            }
        };

        // Send interactive message with the card.
        let content = serde_json::json!({
            "type": "card",
            "data": { "card_id": &card_id }
        })
        .to_string();

        let message_id = self
            .send_message_with_id(chat_id, "interactive", &content)
            .await?;

        // Initialize per-card state.
        {
            let mut state = self.streaming_state.lock().await;
            state.cards.insert(card_id.clone(), CardStreamState {
                last_update_time: None,
                pending_text: None,
                sequence: 1,
                created_at: Instant::now(),
                has_collapsible,
            });
        }

        let draft_id = encode_draft_id(&card_id, &message_id);

        // Track active draft for this chat_id.
        {
            let mut drafts = self.active_drafts.lock().await;
            drafts.insert(chat_id.to_string(), draft_id.clone());
        }

        Ok(Some(draft_id))
    }

    /// Update the streaming card content (throttled per-card to avoid API rate limits).
    pub async fn send_streaming_update(&self, draft_id: &str, text: &str) -> Result<()> {
        let (card_id, _msg_id) = decode_draft_id(draft_id)?;
        let card_id = card_id.to_string();

        let mut state = self.streaming_state.lock().await;

        let card = match state.cards.get_mut(&card_id) {
            Some(c) => c,
            None => return Ok(()), // card already cleaned up
        };

        // Throttle: if less than STREAMING_UPDATE_INTERVAL_MS since last update, save pending.
        let now = Instant::now();
        if let Some(last) = card.last_update_time {
            if now.duration_since(last) < std::time::Duration::from_millis(STREAMING_UPDATE_INTERVAL_MS) {
                card.pending_text = Some(text.to_string());
                return Ok(());
            }
        }

        // Increment sequence number.
        card.sequence += 1;
        let sequence = card.sequence;

        // Clear pending since we're sending the latest text now.
        card.pending_text = None;

        let token = match self.get_tenant_access_token().await {
            Ok(t) => t,
            Err(e) => {
                warn!("streaming update: failed to get token: {e}");
                return Ok(());
            }
        };

        match self.cardkit_update_content(&token, &card_id, text, sequence).await {
            Ok(()) => {
                // Re-acquire card ref after async call (state lock was held across await,
                // but Mutex is held for the entire method).
                if let Some(card) = state.cards.get_mut(&card_id) {
                    card.last_update_time = Some(Instant::now());
                }
            }
            Err(e) => {
                // Token expired during update — retry once.
                if Self::should_refresh_token(reqwest::StatusCode::OK, &serde_json::json!({}))
                    || e.to_string().contains("99991663")
                {
                    self.invalidate_token().await;
                    if let Ok(new_token) = self.get_tenant_access_token().await {
                        if let Err(e2) = self.cardkit_update_content(&new_token, &card_id, text, sequence).await {
                            warn!("streaming update: retry after token refresh failed: {e2}");
                        } else if let Some(card) = state.cards.get_mut(&card_id) {
                            card.last_update_time = Some(Instant::now());
                        }
                    }
                } else {
                    warn!("streaming update: cardkit_update_content failed: {e}");
                }
            }
        }

        Ok(())
    }

    /// Finalize the streaming card: send final content and close streaming mode.
    pub async fn send_streaming_finalize(&self, draft_id: &str, text: &str) -> Result<()> {
        let (card_id, _msg_id) = decode_draft_id(draft_id)?;
        let card_id = card_id.to_string();

        let mut state = self.streaming_state.lock().await;

        let token = match self.get_tenant_access_token().await {
            Ok(t) => t,
            Err(e) => {
                warn!("streaming finalize: failed to get token: {e}");
                state.cards.remove(&card_id);
                return Ok(());
            }
        };

        // Get current sequence (or default).
        let cur_seq = state.cards.get(&card_id).map(|c| c.sequence).unwrap_or(1);

        // Send final content update.
        let update_seq = cur_seq + 1;
        if let Err(e) = self
            .cardkit_update_content(&token, &card_id, text, update_seq)
            .await
        {
            warn!("streaming finalize: final content update failed: {e}");
        }

        // Close streaming mode.
        let close_seq = update_seq + 1;
        let summary = truncate_summary(text, 50);
        if let Err(e) = self
            .cardkit_close_streaming(&token, &card_id, &summary, close_seq)
            .await
        {
            warn!("streaming finalize: cardkit_close_streaming failed: {e}");
        }

        // Clean up.
        state.cards.remove(&card_id);

        // Remove from active drafts.
        {
            let mut drafts = self.active_drafts.lock().await;
            drafts.retain(|_, v| v != draft_id);
        }

        Ok(())
    }

    /// Finalize a streaming card with a collapsible panel for reasoning content.
    /// Replaces the card body with structured JSON elements instead of plain markdown.
    pub async fn send_streaming_finalize_collapsible(
        &self,
        draft_id: &str,
        reasoning: &str,
        tool_trace: &str,
        content: &str,
    ) -> Result<()> {
        let (card_id, _msg_id) = decode_draft_id(draft_id)?;
        let card_id = card_id.to_string();

        let mut state = self.streaming_state.lock().await;

        let token = match self.get_tenant_access_token().await {
            Ok(t) => t,
            Err(e) => {
                warn!("streaming finalize collapsible: failed to get token: {e}");
                state.cards.remove(&card_id);
                return Ok(());
            }
        };

        let cur_seq = state.cards.get(&card_id).map(|c| c.sequence).unwrap_or(1);
        let mut seq = cur_seq;

        // Build the thinking text (reasoning + tool trace), separated by dividers.
        let thinking = match (reasoning.is_empty(), tool_trace.is_empty()) {
            (true, true) => String::new(),
            (false, true) => reasoning.to_string(),
            (true, false) => tool_trace.to_string(),
            (false, false) => format!("{reasoning}\n\n---\n\n{tool_trace}"),
        };

        // Update the thinking_md element inside the collapsible panel.
        if !thinking.is_empty() {
            let sanitized_thinking = sanitize_feishu_markdown(&thinking);
            seq += 1;
            if let Err(e) = self
                .cardkit_update_element(&token, &card_id, "thinking_md", &sanitized_thinking, seq)
                .await
            {
                warn!("streaming finalize collapsible: thinking_md update failed: {e}");
            }
        }

        // Update the content element with clean content (no reasoning blockquotes).
        let sanitized_content = sanitize_feishu_markdown(content);
        seq += 1;
        if let Err(e) = self
            .cardkit_update_content(&token, &card_id, &sanitized_content, seq)
            .await
        {
            warn!("streaming finalize collapsible: content update failed: {e}");
        }

        // Close streaming mode.
        seq += 1;
        let summary = truncate_summary(content, 50);
        if let Err(e) = self
            .cardkit_close_streaming(&token, &card_id, &summary, seq)
            .await
        {
            warn!("streaming finalize collapsible: cardkit_close_streaming failed: {e}");
        }

        // Clean up.
        state.cards.remove(&card_id);

        {
            let mut drafts = self.active_drafts.lock().await;
            drafts.retain(|_, v| v != draft_id);
        }

        Ok(())
    }

    /// Cancel the streaming card: close streaming mode without final content.
    pub async fn send_streaming_cancel(&self, draft_id: &str) -> Result<()> {
        let (card_id, _msg_id) = decode_draft_id(draft_id)?;
        let card_id = card_id.to_string();

        let mut state = self.streaming_state.lock().await;

        let token = match self.get_tenant_access_token().await {
            Ok(t) => t,
            Err(e) => {
                warn!("streaming cancel: failed to get token: {e}");
                state.cards.remove(&card_id);
                return Ok(());
            }
        };

        let cur_seq = state.cards.get(&card_id).map(|c| c.sequence).unwrap_or(1);
        let close_seq = cur_seq + 1;

        if let Err(e) = self
            .cardkit_close_streaming(&token, &card_id, "", close_seq)
            .await
        {
            warn!("streaming cancel: cardkit_close_streaming failed: {e}");
        }

        state.cards.remove(&card_id);

        // Remove from active drafts.
        {
            let mut drafts = self.active_drafts.lock().await;
            drafts.retain(|_, v| v != draft_id);
        }

        Ok(())
    }

    /// Get the active draft ID for a chat, if any.
    pub async fn get_active_draft(&self, chat_id: &str) -> Option<String> {
        let drafts = self.active_drafts.lock().await;
        drafts.get(chat_id).cloned()
    }

    /// Check if a card was created with the collapsible panel layout.
    async fn card_has_collapsible(&self, draft_id: &str) -> bool {
        let (card_id, _) = match decode_draft_id(draft_id) {
            Ok(ids) => ids,
            Err(_) => return false,
        };
        let state = self.streaming_state.lock().await;
        state.cards.get(card_id).map(|c| c.has_collapsible).unwrap_or(false)
    }

    /// Start the streaming bridge: subscribes to the event broadcast channel
    /// and forwards LLM streaming tokens to active Feishu streaming cards.
    ///
    /// This should be spawned as a background task alongside `run_loop`.
    /// When a `token` event arrives for a chat_id that has an active streaming
    /// card, the delta is accumulated and forwarded to CardKit.
    /// When a `message_done` event arrives, the card is finalized.
    pub async fn run_streaming_bridge(
        self: Arc<Self>,
        mut event_rx: tokio::sync::broadcast::Receiver<String>,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) {
        if !self.streaming_enabled {
            info!("Feishu streaming bridge disabled by config");
            return;
        }

        // Per-chat accumulated text for streaming updates.
        let accumulated: Arc<Mutex<HashMap<String, String>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Per-chat accumulated reasoning (thinking) content.
        let reasoning: Arc<Mutex<HashMap<String, String>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Per-chat accumulated tool execution trace (✅/❌ lines).
        let tool_trace: Arc<Mutex<HashMap<String, String>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Stale draft cleanup interval (5 minutes).
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(300));
        cleanup_interval.tick().await; // consume immediate tick

        loop {
            tokio::select! {
                event = event_rx.recv() => {
                    let event_str = match event {
                        Ok(s) => s,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Feishu streaming bridge lagged {n} events");
                            continue;
                        }
                        Err(_) => break,
                    };

                    let parsed: serde_json::Value = match serde_json::from_str(&event_str) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let event_type = parsed.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    let chat_id = parsed.get("chat_id").and_then(|v| v.as_str()).unwrap_or("");

                    if chat_id.is_empty() {
                        continue;
                    }

                    // Check if this chat has an active streaming draft.
                    let draft_id = {
                        let drafts = self.active_drafts.lock().await;
                        drafts.get(chat_id).cloned()
                    };
                    let draft_id = match draft_id {
                        Some(id) => id,
                        None => continue,
                    };

                    match event_type {
                        "thinking" => {
                            let delta = parsed.get("content").and_then(|v| v.as_str()).unwrap_or("");
                            if delta.is_empty() {
                                continue;
                            }
                            debug!(
                                delta_len = delta.len(),
                                chat_id = chat_id,
                                "Feishu streaming bridge received 'thinking' event"
                            );

                            // Accumulate reasoning text.
                            let (reasoning_text, trace_text, content_text) = {
                                let mut reas = reasoning.lock().await;
                                let entry = reas.entry(chat_id.to_string()).or_default();
                                entry.push_str(delta);
                                let r = entry.clone();
                                let tt = tool_trace.lock().await;
                                let t = tt.get(chat_id).cloned().unwrap_or_default();
                                let acc = accumulated.lock().await;
                                let c = acc.get(chat_id).cloned().unwrap_or_default();
                                (r, t, c)
                            };

                            let card_text = compose_streaming_card_full(&reasoning_text, &trace_text, &content_text);

                            if let Err(e) = self.send_streaming_update(&draft_id, &card_text).await {
                                warn!("Feishu streaming bridge thinking update failed: {e}");
                            }
                        }
                        "tool_call_start" => {
                            let tool_name = parsed.get("tool").and_then(|v| v.as_str()).unwrap_or("unknown");

                            // Show tool execution status on the card.
                            let (reasoning_text, trace_text, content_text) = {
                                let reas = reasoning.lock().await;
                                let r = reas.get(chat_id).cloned().unwrap_or_default();
                                let tt = tool_trace.lock().await;
                                let t = tt.get(chat_id).cloned().unwrap_or_default();
                                let acc = accumulated.lock().await;
                                let c = acc.get(chat_id).cloned().unwrap_or_default();
                                (r, t, c)
                            };

                            // Append "calling tool" line to tool trace (shown in thinking block).
                            let live_trace = if trace_text.is_empty() {
                                format!("🔧 正在调用工具: `{tool_name}` ...")
                            } else {
                                format!("{trace_text}\n🔧 正在调用工具: `{tool_name}` ...")
                            };

                            let card_text = compose_streaming_card_full(&reasoning_text, &live_trace, &content_text);

                            if let Err(e) = self.send_streaming_update(&draft_id, &card_text).await {
                                warn!("Feishu streaming bridge tool_call_start update failed: {e}");
                            }
                        }
                        "tool_call_result" => {
                            let tool_name = parsed.get("tool").and_then(|v| v.as_str()).unwrap_or("unknown");
                            let duration_ms = parsed.get("duration_ms").and_then(|v| v.as_u64()).unwrap_or(0);
                            let is_error = parsed.pointer("/result/error").is_some();

                            let status_icon = if is_error { "❌" } else { "✅" };
                            let duration_str = if duration_ms > 1000 {
                                format!("{:.1}s", duration_ms as f64 / 1000.0)
                            } else {
                                format!("{duration_ms}ms")
                            };
                            let status_line = format!("{status_icon} `{tool_name}` ({duration_str})");

                            // Update tool trace: remove "正在调用工具" line, add result line.
                            let (reasoning_text, content_text) = {
                                let reas = reasoning.lock().await;
                                let r = reas.get(chat_id).cloned().unwrap_or_default();
                                let acc = accumulated.lock().await;
                                let c = acc.get(chat_id).cloned().unwrap_or_default();
                                (r, c)
                            };

                            let trace_text = {
                                let mut tt = tool_trace.lock().await;
                                let entry = tt.entry(chat_id.to_string()).or_default();
                                // Remove the "正在调用工具" line if present.
                                if let Some(pos) = entry.rfind("\n🔧 正在调用工具:") {
                                    entry.truncate(pos);
                                    entry.push('\n');
                                } else if entry.starts_with("🔧 正在调用工具:") {
                                    entry.clear();
                                }
                                // Append result line.
                                if !entry.is_empty() && !entry.ends_with('\n') {
                                    entry.push('\n');
                                }
                                entry.push_str(&status_line);
                                entry.clone()
                            };

                            let card_text = compose_streaming_card_full(&reasoning_text, &trace_text, &content_text);

                            if let Err(e) = self.send_streaming_update(&draft_id, &card_text).await {
                                warn!("Feishu streaming bridge tool_call_result update failed: {e}");
                            }
                        }
                        "token" => {
                            let delta = parsed.get("delta").and_then(|v| v.as_str()).unwrap_or("");
                            if delta.is_empty() {
                                continue;
                            }

                            // Accumulate content text (only actual LLM output, no tool status).
                            let (reasoning_text, trace_text, content_text) = {
                                let mut acc = accumulated.lock().await;
                                let entry = acc.entry(chat_id.to_string()).or_default();
                                entry.push_str(delta);
                                let c = entry.clone();
                                let reas = reasoning.lock().await;
                                let r = reas.get(chat_id).cloned().unwrap_or_default();
                                let tt = tool_trace.lock().await;
                                let t = tt.get(chat_id).cloned().unwrap_or_default();
                                (r, t, c)
                            };

                            let card_text = compose_streaming_card_full(&reasoning_text, &trace_text, &content_text);

                            if let Err(e) = self.send_streaming_update(&draft_id, &card_text).await {
                                warn!("Feishu streaming bridge update failed: {e}");
                            }
                        }
                        "message_done" => {
                            let final_text = parsed.get("content").and_then(|v| v.as_str()).unwrap_or("");
                            let event_reasoning = parsed.get("reasoning_content")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");

                            info!(
                                chat_id = chat_id,
                                content_len = final_text.len(),
                                event_reasoning_len = event_reasoning.len(),
                                event_reasoning_preview = truncate_str(event_reasoning, 120),
                                "Feishu streaming bridge received 'message_done'"
                            );

                            // Determine the final content: prefer message_done content,
                            // fall back to accumulated content.
                            let content_text = if !final_text.is_empty() {
                                final_text.to_string()
                            } else {
                                let acc = accumulated.lock().await;
                                acc.get(chat_id).cloned().unwrap_or_default()
                            };

                            // Build the "thinking" block. Priority:
                            // 1. Streaming reasoning (from `thinking` events)
                            // 2. reasoning_content from message_done event
                            // 3. Tool execution trace (✅/❌ lines) as process summary
                            let trace_text = {
                                let tt = tool_trace.lock().await;
                                tt.get(chat_id).cloned().unwrap_or_default()
                            };

                            let reasoning_text = {
                                let reas = reasoning.lock().await;
                                let streamed = reas.get(chat_id).cloned().unwrap_or_default();
                                if streamed.is_empty() {
                                    event_reasoning.to_string()
                                } else {
                                    streamed
                                }
                            };

                            info!(
                                chat_id = chat_id,
                                final_reasoning_len = reasoning_text.len(),
                                trace_len = trace_text.len(),
                                content_len = content_text.len(),
                                "Feishu streaming bridge composing final card"
                            );

                            let text = compose_streaming_card_full(&reasoning_text, &trace_text, &content_text);

                            // Flush any pending throttled text before finalizing.
                            // Use collapsible panel finalization only when:
                            // 1. There's reasoning or tool trace content
                            // 2. The card was created with the collapsible_panel layout
                            let has_thinking = !reasoning_text.is_empty() || !trace_text.is_empty();
                            let use_collapsible = has_thinking && self.card_has_collapsible(&draft_id).await;
                            if !text.is_empty() {
                                if use_collapsible {
                                    if let Err(e) = self.send_streaming_finalize_collapsible(
                                        &draft_id, &reasoning_text, &trace_text, &content_text,
                                    ).await {
                                        warn!("Feishu streaming bridge collapsible finalize failed: {e}");
                                    }
                                } else if let Err(e) = self.send_streaming_finalize(&draft_id, &text).await {
                                    warn!("Feishu streaming bridge finalize failed: {e}");
                                }
                            } else if let Err(e) = self.send_streaming_cancel(&draft_id).await {
                                warn!("Feishu streaming bridge cancel failed: {e}");
                            }

                            // Clean up all per-chat state.
                            {
                                let mut acc = accumulated.lock().await;
                                acc.remove(chat_id);
                            }
                            {
                                let mut reas = reasoning.lock().await;
                                reas.remove(chat_id);
                            }
                            {
                                let mut tt = tool_trace.lock().await;
                                tt.remove(chat_id);
                            }
                        }
                        _ => {}
                    }
                }
                _ = cleanup_interval.tick() => {
                    // Clean up stale drafts (cards open for more than 10 minutes).
                    let stale_threshold = std::time::Duration::from_secs(600);
                    let stale_cards: Vec<String> = {
                        let state = self.streaming_state.lock().await;
                        state.cards.iter()
                            .filter(|(_, card)| card.created_at.elapsed() > stale_threshold)
                            .map(|(id, _)| id.clone())
                            .collect()
                    };

                    if !stale_cards.is_empty() {
                        warn!(count = stale_cards.len(), "Cleaning up stale streaming cards");
                        let stale_drafts: Vec<String> = {
                            let drafts = self.active_drafts.lock().await;
                            drafts.iter()
                                .filter(|(_, draft_id)| {
                                    decode_draft_id(draft_id)
                                        .map(|(cid, _)| stale_cards.contains(&cid.to_string()))
                                        .unwrap_or(false)
                                })
                                .map(|(_, draft_id)| draft_id.clone())
                                .collect()
                        };
                        for draft_id in stale_drafts {
                            if let Err(e) = self.send_streaming_cancel(&draft_id).await {
                                warn!("Failed to cancel stale streaming card: {e}");
                            }
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("Feishu streaming bridge shutting down");
                    break;
                }
            }
        }
    }
}

/// Fetch a fresh tenant_access_token from Feishu API.
async fn fetch_tenant_access_token(
    client: &Client,
    app_id: &str,
    app_secret: &str,
) -> Result<(String, i64)> {
    #[derive(Serialize)]
    struct TokenRequest<'a> { app_id: &'a str, app_secret: &'a str }

    let resp = client
        .post(format!("{}/auth/v3/tenant_access_token/internal", FEISHU_OPEN_API))
        .json(&TokenRequest { app_id, app_secret })
        .send()
        .await
        .map_err(|e| Error::Channel(format!("Failed to get Feishu access token: {}", e)))?;

    let body: TokenResponse = resp
        .json()
        .await
        .map_err(|e| Error::Channel(format!("Failed to parse Feishu token response: {}", e)))?;

    if body.code != 0 {
        return Err(Error::Channel(format!("Feishu token error: {}", body.msg)));
    }

    let token = body
        .tenant_access_token
        .ok_or_else(|| Error::Channel("No access token in Feishu response".to_string()))?;
    let expires_in = body.expire.unwrap_or(7200).max(60);
    Ok((token, expires_in))
}

/// Get a cached tenant_access_token for the free send_message function.
async fn get_cached_token(config: &Config) -> Result<String> {
    let cache = global_token_cache();
    let mut guard = cache.lock().await;
    if guard.is_valid() {
        return Ok(guard.token.clone());
    }
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| Error::Channel(format!("Failed to build HTTP client: {}", e)))?;
    let (token, expires_in) = fetch_tenant_access_token(
        &client,
        &config.channels.feishu.app_id,
        &config.channels.feishu.app_secret,
    )
    .await?;
    guard.token = token.clone();
    guard.expires_at = chrono::Utc::now().timestamp() + expires_in;
    info!(expires_in = expires_in, "Feishu tenant_access_token refreshed via global cache");
    Ok(token)
}

fn is_feishu_token_invalid_error(s: &str) -> bool {
    // Feishu OpenAPI: 99991663 Invalid access token for authorization
    // Be defensive: match code and message substrings.
    s.contains("99991663")
        || s.contains("Invalid access token")
        || s.contains("invalid access token")
        || s.contains("token attached")
}

async fn invalidate_global_token_cache() {
    let cache = global_token_cache();
    let mut guard = cache.lock().await;
    guard.token.clear();
    guard.expires_at = 0;
}

pub async fn send_message(config: &Config, chat_id: &str, text: &str) -> Result<()> {
    crate::rate_limit::feishu_limiter().acquire().await;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| Error::Channel(format!("Failed to build HTTP client: {}", e)))?;

    let token = get_cached_token(config).await?;
    match do_send_message(&client, &token, chat_id, text).await {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            if is_feishu_token_invalid_error(&msg) {
                warn!("Feishu send_message got invalid token error, refreshing token and retrying once");
                invalidate_global_token_cache().await;
                let token2 = get_cached_token(config).await?;
                return do_send_message(&client, &token2, chat_id, text).await;
            }
            Err(e)
        }
    }
}

/// Upload a local file to Feishu and return the resource key.
/// Images → /im/v1/images (returns image_key)
/// Other  → /im/v1/files  (returns file_key)
async fn upload_feishu_media(
    client: &Client,
    token: &str,
    file_path: &str,
    file_type: &str,
) -> Result<String> {
    let path = std::path::Path::new(file_path);
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file")
        .to_string();

    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| Error::Channel(format!("Failed to read file {}: {}", file_path, e)))?;

    let mime = feishu_mime_for_path(file_path);
    let is_image = file_type == "image";

    if is_image {
        let part = reqwest::multipart::Part::bytes(bytes)
            .file_name(file_name)
            .mime_str(mime)
            .map_err(|e| Error::Channel(format!("Invalid MIME: {}", e)))?;
        let form = reqwest::multipart::Form::new()
            .text("image_type", "message")
            .part("image", part);

        #[derive(Deserialize)]
        struct Resp { code: i32, msg: String, data: Option<ImgData> }
        #[derive(Deserialize)]
        struct ImgData { image_key: String }

        let resp = client
            .post(format!("{}/im/v1/images", FEISHU_OPEN_API))
            .header("Authorization", format!("Bearer {}", token))
            .multipart(form)
            .send()
            .await
            .map_err(|e| Error::Channel(format!("Feishu image upload failed: {}", e)))?;

        let r: Resp = resp.json().await
            .map_err(|e| Error::Channel(format!("Feishu image upload parse failed: {}", e)))?;
        if r.code != 0 {
            return Err(Error::Channel(format!("Feishu image upload error {}: {}", r.code, r.msg)));
        }
        return r.data.map(|d| d.image_key)
            .ok_or_else(|| Error::Channel("Feishu image upload: no image_key".to_string()));
    }

    let part = reqwest::multipart::Part::bytes(bytes)
        .file_name(file_name.clone())
        .mime_str(mime)
        .map_err(|e| Error::Channel(format!("Invalid MIME: {}", e)))?;
    let form = reqwest::multipart::Form::new()
        .text("file_type", file_type.to_string())
        .text("file_name", file_name)
        .part("file", part);

    #[derive(Deserialize)]
    struct Resp { code: i32, msg: String, data: Option<FileData> }
    #[derive(Deserialize)]
    struct FileData { file_key: String }

    let resp = client
        .post(format!("{}/im/v1/files", FEISHU_OPEN_API))
        .header("Authorization", format!("Bearer {}", token))
        .multipart(form)
        .send()
        .await
        .map_err(|e| Error::Channel(format!("Feishu file upload failed: {}", e)))?;

    let r: Resp = resp.json().await
        .map_err(|e| Error::Channel(format!("Feishu file upload parse failed: {}", e)))?;
    if r.code != 0 {
        return Err(Error::Channel(format!("Feishu file upload error {}: {}", r.code, r.msg)));
    }
    r.data.map(|d| d.file_key)
        .ok_or_else(|| Error::Channel("Feishu file upload: no file_key".to_string()))
}

fn feishu_mime_for_path(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "opus" => "audio/ogg",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "m4a" => "audio/mp4",
        "amr" => "audio/amr",
        "mp4" => "video/mp4",
        "pdf" => "application/pdf",
        "doc" => "application/msword",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls" => "application/vnd.ms-excel",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "ppt" => "application/vnd.ms-powerpoint",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "zip" => "application/zip",
        "txt" => "text/plain",
        _ => "application/octet-stream",
    }
}

fn feishu_file_type_for_ext(ext: &str) -> &'static str {
    match ext {
        "jpg" | "jpeg" | "png" | "gif" | "webp" | "bmp" => "image",
        "opus" | "amr" | "mp3" | "wav" | "m4a" => "opus",
        "mp4" | "avi" | "mov" | "mkv" => "mp4",
        "pdf" => "pdf",
        "doc" | "docx" => "doc",
        "xls" | "xlsx" => "xls",
        "ppt" | "pptx" => "ppt",
        _ => "stream",
    }
}

/// Send a media message (image/audio/video/file) to a Feishu chat.
/// Uploads the file first, then sends the appropriate message type.
pub async fn send_media_message(config: &Config, chat_id: &str, file_path: &str) -> Result<()> {
    crate::rate_limit::feishu_limiter().acquire().await;

    let ext = file_path.rsplit('.').next().unwrap_or("").to_lowercase();
    let file_type = feishu_file_type_for_ext(&ext);
    let is_image = file_type == "image";

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .map_err(|e| Error::Channel(format!("Failed to build HTTP client: {}", e)))?;
    let token = get_cached_token(config).await?;

    info!(file_path = %file_path, file_type = %file_type, "Feishu: uploading media");
    let key = match upload_feishu_media(&client, &token, file_path, file_type).await {
        Ok(k) => k,
        Err(e) => {
            let msg = e.to_string();
            if is_feishu_token_invalid_error(&msg) {
                warn!("Feishu send_media_message upload got invalid token error, refreshing token and retrying once");
                invalidate_global_token_cache().await;
                let token2 = get_cached_token(config).await?;
                upload_feishu_media(&client, &token2, file_path, file_type).await?
            } else {
                return Err(e);
            }
        }
    };
    info!(key = %key, "Feishu: media uploaded");

    let (msg_type, content) = if is_image {
        ("image", serde_json::json!({ "image_key": key }).to_string())
    } else if matches!(ext.as_str(), "opus" | "amr" | "mp3" | "wav" | "m4a") {
        ("audio", serde_json::json!({ "file_key": key }).to_string())
    } else if matches!(ext.as_str(), "mp4" | "avi" | "mov" | "mkv") {
        ("media", serde_json::json!({ "file_key": key }).to_string())
    } else {
        ("file", serde_json::json!({ "file_key": key }).to_string())
    };

    #[derive(Serialize)]
    struct SendReq<'a> {
        receive_id: &'a str,
        msg_type: &'a str,
        content: String,
    }

    let send_client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| Error::Channel(format!("Failed to build HTTP client: {}", e)))?;

    async fn send_once(
        send_client: &Client,
        token: &str,
        chat_id: &str,
        msg_type: &str,
        content: &str,
    ) -> Result<()> {
        let resp = send_client
            .post(format!("{}/im/v1/messages?receive_id_type=chat_id", FEISHU_OPEN_API))
            .header("Authorization", format!("Bearer {}", token))
            .json(&SendReq {
                receive_id: chat_id,
                msg_type,
                content: content.to_string(),
            })
            .send()
            .await
            .map_err(|e| Error::Channel(format!("Feishu send_media_message failed: {}", e)))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Channel(format!("Feishu API send media error: {}", body)));
        }
        Ok(())
    }

    match send_once(&send_client, &token, chat_id, msg_type, &content).await {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            if is_feishu_token_invalid_error(&msg) {
                warn!("Feishu send_media_message got invalid token error, refreshing token and retrying once");
                invalidate_global_token_cache().await;
                let token2 = get_cached_token(config).await?;
                return send_once(&send_client, &token2, chat_id, msg_type, &content).await;
            }
            Err(e)
        }
    }
}

async fn do_send_message(client: &Client, token: &str, chat_id: &str, text: &str) -> Result<()> {
    #[derive(Serialize)]
    struct SendMessageRequest<'a> {
        receive_id: &'a str,
        msg_type: &'a str,
        content: String,
    }

    let content = serde_json::json!({ "text": text }).to_string();
    let request = SendMessageRequest {
        receive_id: chat_id,
        msg_type: "text",
        content,
    };

    let response = client
        .post(format!("{}/im/v1/messages?receive_id_type=chat_id", FEISHU_OPEN_API))
        .header("Authorization", format!("Bearer {}", token))
        .json(&request)
        .send()
        .await
        .map_err(|e| Error::Channel(format!("Failed to send Feishu message: {}", e)))?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(Error::Channel(format!("Feishu API error: {}", body)));
    }
    Ok(())
}
