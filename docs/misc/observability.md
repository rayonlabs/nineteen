# Observability

As a part of the Validator deployment, A full observability stack is provided. Make sure to set the following vars:

```bash
# configure the HTTP proxy
SERVER_BASE_URL=http://111.222.333.444
SERVER_PORT=80                          # defaults to 80, but the proxy will bind to this port

# configure grafana
GRAFANA_USERNAME=foobar                 # defaults to `admin`. You might want something else.
GRAFANA_PASSWORD=hunter2                # defaults to `admin`. Change this, make it secure!
```

After starting the Validator, head to `http://<server_ip>:<server_port>/grafana`
and log in with `GRAFANA_USERNAME` and `GRAFANA_PASSWORD`. You can find all of
the Prometheus Metrics in the Metrics tab:

![Grafana Metrics](grafana_metrics.png)

Go to the `Dashboards` tab to explore some pre-built dashboards. The `Validator Errors` dashboard is useful to quickly diagnose issues.

![Grafana Dashboards](grafana_dashboards.png)