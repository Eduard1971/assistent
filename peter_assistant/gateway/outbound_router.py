class OutboundRouter:
    def __init__(self, channels: dict): self.channels=channels
    async def send(self, route, subject, body, attachments=None):
        channel=self.channels.get(route.channel)
        if not channel: raise RuntimeError(f"Channel {route.channel} nie je nakonfigurovaný")
        return await channel.send(route,subject,body,attachments)
