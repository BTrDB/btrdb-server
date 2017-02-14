# Load shedding resource manager

You are likely here because you saw an error message like

```
[426] The cluster is underprovisioned and is shedding load. You drew the short straw. Sorry.
```

The correct response to this message is to retry your request using an exponential backoff
time, and report to your BTrDB cluster administrator that they need to provision more resources.
