from netaddr import valid_ipv4, valid_ipv6
import re


def identify_if_ip_is_src_or_dest(ipType):
    if ipType == "source":
        return "src"
    else:
        return "dest"


def identify_if_ip_is_ip4_or_ip6(ip):
    if valid_ipv4(ip):
        return "ipv4"
    elif valid_ipv6(ip):
        return "ipv6"
    else:
        return "NA"


def get_src_dst_ips_for_msexchange(record):
    srcIPKey = "client-ip"
    dstIPKey = "server-ip"
    return record[srcIPKey], record[dstIPKey]


def get_src_dst_hostname_for_msexchange(record):
    srcHostKey = "client-hostname"
    dstHostKey = "server-hostname"
    return record[srcHostKey], record[dstHostKey]


def get_src_dst_username_for_msexchange(record):
    srcUserKey = "sender-address"
    dstUserLey = "recipient-address"
    return [record[srcUserKey], record[dstUserLey]]


def get_ip_format_for_msexchange(record):
    src_ip, dst_ip = get_src_dst_ips_for_msexchange(record)

    src_ip_fmt = identify_if_ip_is_ip4_or_ip6(src_ip) if src_ip != "" else "NA"
    dst_ip_fmt = identify_if_ip_is_ip4_or_ip6(dst_ip) if dst_ip != "" else "NA"

    return [src_ip_fmt, dst_ip_fmt]
