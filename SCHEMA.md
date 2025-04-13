# Cassandra Schema Design for Messenger MVP

## Table 1: messages_by_conversation

**Primary Key:** (conversation_id, message_ts)

```cql
CREATE TABLE IF NOT EXISTS messages_by_conversation (
    conversation_id UUID,
    message_ts TIMESTAMP,
    sender_id UUID,
    content TEXT,
    PRIMARY KEY (conversation_id, message_ts)
) WITH CLUSTERING ORDER BY (message_ts DESC);
```

### Purpose:

* Store messages in a conversation.
* Support fetching all messages in a conversation.
* Support pagination using `message_ts`.

---

## Table 2: conversations_by_user

**Primary Key:** (user_id, last_message_ts)

```cql
CREATE TABLE IF NOT EXISTS conversations_by_user (
    user_id UUID,
    last_message_ts TIMESTAMP,
    conversation_id UUID,
    peer_id UUID,
    PRIMARY KEY (user_id, last_message_ts)
) WITH CLUSTERING ORDER BY (last_message_ts DESC);
```

### Purpose:

* Store active conversations for a user.
* Support fetching conversations ordered by recent activity.
* `peer_id` helps identify the other participant.

---

## Notes:

* Use `UUID` for all IDs.
* Use `message_ts` as a cursor for pagination (no offset).
* Designed specifically for required read patterns in the assignment.
