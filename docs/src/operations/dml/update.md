# UPDATE SET

Currently, update only supports for Spark 3.5+.

Update with condition:

```sql
UPDATE users 
SET name = 'Updated Name' 
WHERE id = 4;
```

Update with complex data types:

```sql
UPDATE events 
SET metadata = named_struct('source', 'ios', 'version', 1, 'processed_at', timestamp'2024-01-15 13:00:00') 
WHERE event_id = 1001;
```

Update struct's field:

```sql
UPDATE events 
SET metadata = named_struct('source', metadata.source, 'version', 2, 'processed_at', timestamp'2024-01-15 13:00:00') 
WHERE event_id = 1001;
```

Update array field:

```sql
UPDATE events
SET tags = ARRAY('ios', 'mobile')
WHERE event_id = 1001;
```

