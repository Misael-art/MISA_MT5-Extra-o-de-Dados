(function () {
  "use strict";
  // Conserva a grafia numérica do JSON canônico (0.0, -0.0, expoentes).
  // JSON.stringify dos fatos mudaria esses bytes e invalidaria seu digest.
  function sourceJSON(raw, digest, integrity) {
    const marker = '"snapshot_id":null';
    let depth = 0, quoted = false, escaped = false, at = -1;
    for (let i = 0; i < raw.length; i++) {
      const char = raw[i];
      if (quoted) {
        if (escaped) escaped = false;
        else if (char === "\\") escaped = true;
        else if (char === '"') quoted = false;
      } else if (char === '"') {
        if (depth === 1 && raw.startsWith(marker, i)) {at = i; break;}
        quoted = true;
      } else if (char === "{") depth++;
      else if (char === "}") depth--;
    }
    if (at < 0) throw Error("Identidade canônica ausente.");
    const body = raw.slice(0, at) + '"snapshot_id":' + JSON.stringify(digest) + raw.slice(at + marker.length);
    return body.slice(0, -1) + ',"integrity":' + JSON.stringify(integrity) + "}";
  }
  async function decode(encoded) {
    if (encoded.encoding !== "base64-utf8-canonical-json") throw Error("Codificação desconhecida.");
    const bytes = Uint8Array.from(atob(encoded.content), char => char.charCodeAt(0));
    if (!globalThis.crypto || !crypto.subtle) throw Error("Navegador sem verificação SHA256; use um navegador atualizado.");
    const digest = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)))
      .map(byte => byte.toString(16).padStart(2, "0")).join("");
    if (encoded.integrity.algorithm !== "sha256" || digest !== encoded.integrity.digest) throw Error("Digest da fotografia divergente.");
    const raw = new TextDecoder("utf-8", {fatal: true}).decode(bytes);
    const data = JSON.parse(raw);
    if (data.schema_version !== 1 || data.snapshot_id !== null || !Array.isArray(data.sections))
      throw Error("Schema de fotografia não suportado.");
    data.snapshot_id = digest;
    data.integrity = encoded.integrity;
    return {data, json: sourceJSON(raw, digest, encoded.integrity)};
  }
  window.MT5XTransport = {decode};
}());
