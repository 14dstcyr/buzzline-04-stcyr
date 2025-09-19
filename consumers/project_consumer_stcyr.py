"""
project_consumer_stcyr.py

Consume JSON messages from a Kafka topic and visualize sentiment trends in real-time.

JSON messages have several attributes including:
- message
- author
- timestamp
- category
- sentiment
- keyword_mentioned
- message_length
"""

#####################################
# Import Modules
#####################################

# Python Standard Library
import os
import json
from collections import deque
from datetime import datetime

# External Packages
from dotenv import load_dotenv
import matplotlib.pyplot as plt

# Local Utilities
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Data Structures
#####################################

# Rolling window for last N points
MAX_POINTS = 50
timestamps = deque(maxlen=MAX_POINTS)
sentiments = deque(maxlen=MAX_POINTS)

#####################################
# Chart Setup
#####################################

fig, ax = plt.subplots()
plt.ion()  # interactive mode on

#####################################
# Update Chart Function
#####################################


def update_chart():
    """Update the live line chart with new sentiment data."""
    ax.clear()
    ax.plot(timestamps, sentiments, color="blue", marker="o", linestyle="-")

    ax.set_xlabel("Time")
    ax.set_ylabel("Sentiment (-1 to 1)")
    ax.set_title("Streaming Sentiment Trend - Deb St. Cyr")

    # Format x-axis labels
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    plt.draw()
    plt.pause(0.01)


#####################################
# Process a Message
#####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        logger.debug(f"Raw message: {message}")
        message_dict: dict = json.loads(message)

        if isinstance(message_dict, dict):
            sentiment = message_dict.get("sentiment")
            timestamp_str = message_dict.get("timestamp")

            if sentiment is not None and timestamp_str:
                ts = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                timestamps.append(ts)
                sentiments.append(sentiment)

                logger.info(f"Message at {timestamp_str} → sentiment={sentiment}")
                print(f"✅ Added point → {timestamp_str} : {sentiment}")  # debug print
                update_chart()

            else:
                logger.warning(f"Missing sentiment or timestamp: {message_dict}")
        else:
            logger.error(f"Expected dict but got {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main Function
#####################################


def main() -> None:
    """Main entry point for the consumer."""
    logger.info("START project_consumer_stcyr")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consuming from topic '{topic}' with group '{group_id}'")

    consumer = create_kafka_consumer(topic, group_id)

    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

    logger.info("END project_consumer_stcyr")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
