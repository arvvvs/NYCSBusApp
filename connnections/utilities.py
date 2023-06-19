def check_if_ec2() -> bool:
    import socket

    hostname = socket.gethostname()

    if hostname.startswith("ip-") or hostname.startswith("ec2-"):
        print(hostname)
        print(True)
        return True
    else:
        return False
