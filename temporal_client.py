from temporalio.client import Client
import os
from dotenv import load_dotenv

load_dotenv()

async def get_temporal_client():
    print("📬 Connecting to Temporal Cloud...")

    return await Client.connect(
        target_host=os.getenv("TEMPORAL_ADDRESS"),   # you called it TEMPORAL_ADDRESS in your .env
        namespace=os.getenv("TEMPORAL_NAMESPACE"),
        api_key=os.getenv("TEMPORAL_API_KEY"),
        tls=True
    )
