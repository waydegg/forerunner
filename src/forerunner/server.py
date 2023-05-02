from uvicorn.server import Server


class MetricsServer(Server):
    def install_signal_handlers(self):
        pass
