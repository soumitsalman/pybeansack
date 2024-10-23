from fastapi import FastAPI, Request
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_ui.handler import slack_app

app = FastAPI(title="Espresso (News) on Slack", version="0.0.1", description="Slack bot for Espresso (Alpha)", root_path="/slack")
handler = SlackRequestHandler(slack_app)

@app.post("/events")
@app.post("/commands")
@app.post("/actions")
@app.get("/oauth-redirect")
@app.get("/install")
async def receive_slack_app_events(req: Request):
    res = await handler.handle(req)
    return res