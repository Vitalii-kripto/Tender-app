import logging
import requests

logger = logging.getLogger("AutoSSH")

class RfProxyTunnelConfig:
    def __init__(self, ssh_host, ssh_user, local_socks_port, allowed_domains=None, warmup_url=None):
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.local_socks_port = local_socks_port
        self.allowed_domains = allowed_domains
        self.warmup_url = warmup_url

    def ensure(self):
        # В реальной системе здесь запуск SSH туннеля
        logger.info(f"Ensuring SSH tunnel to {self.ssh_host} on port {self.local_socks_port}")
        pass

class RfProxyHttpClient:
    def __init__(self, cfg: RfProxyTunnelConfig):
        self.cfg = cfg
        self.tunnel = cfg
        self.session = requests.Session()
        if cfg.local_socks_port:
            self.session.proxies = {
                'http': f'socks5h://127.0.0.1:{cfg.local_socks_port}',
                'https': f'socks5h://127.0.0.1:{cfg.local_socks_port}'
            }

    def ensure(self):
        self.cfg.ensure()

    def warmup(self):
        if self.cfg.warmup_url:
            logger.info(f"Warming up proxy via {self.cfg.warmup_url}")
            try:
                self.session.get(self.cfg.warmup_url, timeout=10)
            except Exception as e:
                logger.error(f"Warmup failed: {e}")

    def close(self):
        self.session.close()
