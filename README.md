# Rotten Multistream Bridge

Bridge pensado para dos cosas:

- conectar `TikTok` directo con `tiktok-live-connector`
- servir un feed unificado para un widget de StreamElements por `GET /events`

Tambien mantiene compatibilidad basica con tu app Android por `Socket.IO` usando `join`.

## Endpoints

- `GET /health`
- `GET /events?since=0&limit=30`
- `POST /ingest`
- `POST /sources/tiktok/connect`
- `POST /sources/tiktok/disconnect`

## Variables de entorno

- `PORT`
- `ALLOW_ORIGIN`
- `EVENT_BUFFER_SIZE`
- `TIKTOK_USERNAME`
- `TIKTOK_LIKE_MILESTONE`
- `INGEST_TOKEN`
- `SOCKET_JOIN_PUBLISH`

## Render

1. Sube esta carpeta a GitHub.
2. Crea un `Web Service` en Render.
3. Usa `npm install` como build command.
4. Usa `npm start` como start command.
5. Si quieres TikTok fijo, agrega `TIKTOK_USERNAME`.
6. Si quieres proteger `POST /ingest`, agrega `INGEST_TOKEN`.

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
