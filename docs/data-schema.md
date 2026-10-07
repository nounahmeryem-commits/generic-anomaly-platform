# Generic Data Schema

## Objective

Define a generic data format that can be used by different sectors
(manufacturing, transport, energy, IT, etc.).

## Generic Event

Each incoming event contains the following fields:

| Field | Type | Required | Description |
|---|---|---|---|
| entity_id | string | Yes | Identifier of the monitored entity |
| timestamp | datetime | Yes | Date and time of the measurement |
| sensor_type | string | Yes | Type of measurement or sensor |
| value | float | Yes | Measured value |
| location | string | No | Location of the entity |

## Example

```json
{
  "entity_id": "entity_001",
  "timestamp": "2026-09-30T10:00:00Z",
  "sensor_type": "temperature",
  "value": 72.5,
  "location": "site_A"
}
