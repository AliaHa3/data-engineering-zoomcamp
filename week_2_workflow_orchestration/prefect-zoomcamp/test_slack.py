from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("slack-block-name")
slack_webhook_block.notify("Hello from Prefect!")