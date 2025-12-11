# test_logs.py
import asyncio
import websockets
import json
import requests


async def test_specific_run(run_id: int):
    """Тестируем конкретный run"""
    url = f"ws://localhost:8000/runs/{run_id}/logs/ws"

    print(f"\n{'=' * 60}")
    print(f"Testing run_id: {run_id}")
    print(f"URL: {url}")
    print('=' * 60)

    try:
        async with websockets.connect(url) as ws:
            print("✅ Connected!")

            # Получаем все сообщения
            try:
                while True:
                    message = await ws.recv()
                    print(message, end="")
            except websockets.exceptions.ConnectionClosed as e:
                print(f"\nConnection closed: {e.code} - {e.reason}")

    except Exception as e:
        print(f"❌ Error: {type(e).__name__}: {e}")


async def test_multiple_runs():
    """Тестируем несколько runs с разными статусами"""
    # Тестовые runs с вашего кластера
    test_runs = [
        1,  # run-a55597b4 (Completed 3h42m ago)
        2,  # run-af73c884 (Error 8m ago) - недавний
        999  # Несуществующий
    ]

    for run_id in test_runs:
        await test_specific_run(run_id)
        await asyncio.sleep(1)


async def create_new_job_and_test():
    """Создаем новый job и тестируем стриминг в реальном времени"""
    print("\n" + "=" * 60)
    print("Creating new job and testing real-time streaming")
    print("=" * 60)

    # 1. Создаем новый job через ваш API
    script_content = """#!/bin/bash
echo "Starting real-time test script..."
for i in {1..10}; do
    echo "Log line $i: $(date '+%H:%M:%S')"
    sleep 2
done
echo "Script completed!"
"""

    response = requests.post(
        "http://localhost:8000/api/scripts",  # Ваш endpoint для создания скриптов
        json={
            "name": "realtime-test",
            "script_content": script_content,
            "created_by": "test-client"
        }
    )

    if response.status_code != 200:
        print(f"Failed to create script: {response.text}")
        return

    script_data = response.json()
    new_run_id = script_data["id"]
    print(f"Created new run with ID: {new_run_id}")

    # 2. Ждем немного, чтобы job начал выполняться
    print("Waiting 5 seconds for job to start...")
    await asyncio.sleep(5)

    # 3. Подключаемся к логам
    await test_specific_run(new_run_id)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Тестируем конкретный run_id
        run_id = int(sys.argv[1])
        asyncio.run(test_specific_run(run_id))
    elif len(sys.argv) > 1 and sys.argv[1] == "new":
        # Создаем новый job и тестируем
        asyncio.run(create_new_job_and_test())
    else:
        # Тестируем несколько runs
        asyncio.run(test_multiple_runs())