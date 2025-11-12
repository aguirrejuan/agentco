"""
CLI interface for AgentCo data analysis agent using Typer.
"""

from pathlib import Path

import typer
from dotenv import load_dotenv
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

# Load environment variables
load_dotenv()

app = typer.Typer(
    name="agentco", help="AgentCo Data Analysis Agent CLI", add_completion=False
)


@app.command()
async def generate_report(
    folder_data: Path = typer.Option(
        ..., exists=True, dir_okay=True, file_okay=False, readable=True
    )
):
    """
    Generate the consolidate report of data quality issues of each data source
    """

    APP_NAME = "weather_app"
    USER_ID = "1234"
    SESSION_ID = "session12343"

    # Step 5: Session and Runner setup
    session_service = InMemorySessionService()
    runner = Runner(
        agent=root_agent, app_name=APP_NAME, session_service=session_service
    )

    # Create the session asynchronously (use await directly in notebook)
    print(f"Creating session with ID: {SESSION_ID}")
    session = await session_service.create_session(
        app_name=APP_NAME, user_id=USER_ID, session_id=SESSION_ID
    )
    print(f"Session created: {session}")

    # Agent Interaction
    content = types.Content(
        role="user",
        parts=[
            types.Part(
                text="Give me a summary of the data quality issues detected today."
            )
        ],
    )

    events = runner.run(user_id=USER_ID, session_id=SESSION_ID, new_message=content)

    for event in events:
        print(f"\nDEBUG EVENT: {event}\n")
        if event.is_final_response() and event.content:
            final_answer = event.content.parts[0].text.strip()
            print("\n🟢 FINAL ANSWER\n", final_answer, "\n")


if __name__ == "__main__":
    app()
