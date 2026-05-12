# Rotten Multistream Bridge

Bridge pensado para dos cosas:

- conectar `TikTok` directo con `tiktok-live-connector`
- conectar `YouTube`, `Twitch` y `Kick` directo desde el bridge
- servir un feed unificado para un widget de StreamElements por `GET /events`

Tambien mantiene compatibilidad basica con tu app Android por `Socket.IO` usando `join`.

## Endpoints

- `GET /health`
- `GET /events?since=0&limit=30`
- `POST /ingest`
- `POST /sources/configure`
- `POST /sources/disconnect-all`
- `POST /sources/tiktok/connect`
- `POST /sources/tiktok/disconnect`
- `POST /sources/youtube/connect`
- `POST /sources/youtube/disconnect`
- `POST /sources/twitch/connect`
- `POST /sources/twitch/disconnect`
- `POST /sources/kick/connect`
- `POST /sources/kick/disconnect`

## Variables de entorno

- `PORT`
- `ALLOW_ORIGIN`
- `EVENT_BUFFER_SIZE`
- `TIKTOK_USERNAME`
- `YOUTUBE_LIVE_ID`
- `TWITCH_CHANNEL`
- `KICK_CHANNEL`
- `TIKTOK_LIKE_MILESTONE`
- `INGEST_TOKEN`
- `SOCKET_JOIN_PUBLISH`

## Render

1. Sube esta carpeta a GitHub.
2. Crea un `Web Service` en Render.
3. Usa `npm install` como build command.
4. Usa `npm start` como start command.
5. Si quieres TikTok fijo, agrega `TIKTOK_USERNAME`.
6. Si quieres YouTube fijo, agrega `YOUTUBE_LIVE_ID`.
7. Si quieres Twitch fijo, agrega `TWITCH_CHANNEL`.
8. Si quieres Kick fijo, agrega `KICK_CHANNEL`.
9. Si quieres proteger `POST /ingest` y `POST /sources/*`, agrega `INGEST_TOKEN`.

## Configurar desde el widget

El widget puede mandar un `POST /sources/configure` con este formato:

```json
{
  "tiktokUsername": "@rottenbyte",
  "youtubeLiveId": "abc123LIVE",
  "twitchChannel": "rottenbyte",
  "kickChannel": "rottenbyte"
}
```

Si `INGEST_TOKEN` esta definido, el widget debe mandar `x-ingest-token`.

## Payload esperado para `/ingest`

Evento unico:

```json
{
  "source": "twitch",
  "kind": "chat",
  "type": "message",
  "user": "rottenbro",
  "displayName": "RottenBro",
  "message": "hola chat"
}
```

Multiples eventos:

```json
{
  "events": [
    {
      "source": "youtube",
      "kind": "alert",
      "type": "superchat",
      "user": "miguelYT",
      "amount": "$5.00",
      "message": "$5.00",
      "importance": "high"
    },
    {
      "source": "kick",
      "kind": "chat",
      "type": "message",
      "user": "kicklord",
      "message": "saludos"
    }
  ]
}
```

Si definiste `INGEST_TOKEN`, mandalo en `x-ingest-token` o `Authorization: Bearer ...`.
