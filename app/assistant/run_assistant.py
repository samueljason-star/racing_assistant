"""Placeholder for a future OpenAI-powered assistant."""

from app.assistant.tools import run_full_update


def run_assistant_command(command: str) -> None:
    if command.strip().lower() == "update today":
        run_full_update()
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    run_assistant_command("update today")
