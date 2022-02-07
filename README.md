# daggre

**DAta-AGGREgator**, a tool to handle aggregation on lists of dict-data:

```python
data = [
    {'field1': 'intvalue'},
    {'field2': 'strvalue'},
    {'field3': ['listvalue']},
    {'field4': {'dictkey': 'dictvalue'}}
]
```

which can be used in these scenarios:

- filter and join between table/config data rows
- map-reduce for medium/low scale data

