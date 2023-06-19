def check_if_ec2() -> bool:
    import socket

    hostname = socket.gethostname()

    if hostname.startswith("ip-") or hostname.startswith("ec2-"):
        return True
    else:
        return False
