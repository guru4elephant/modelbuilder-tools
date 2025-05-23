#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
百度云API签名工具类
本工具用于百度云API的签名认证，适用于基于Access Key的鉴权方式，
且最终认证字符串为bce-auth-v{version}/{accessKeyId}/{timestamp}/{expirationPeriodInSeconds}/{signedHeaders}/{signature}的API。
"""

import hashlib
import hmac
import time
import requests
import json
from urllib.parse import quote
from typing import Dict, Any, Optional


class BceApiSignatureTool:
    """百度云API签名工具类"""

    def __init__(self, ak: str, sk: str, host: str, expiration_seconds: int = 1800):
        """
        初始化签名工具

        Args:
            ak: Access Key ID
            sk: Secret Access Key
            host: API域名
            expiration_seconds: 签名有效期（秒）
        """
        self.ak = ak
        self.sk = sk
        self.host = host
        self.expiration_seconds = expiration_seconds

    def _generate_canonical_headers(self, headers: Dict[str, str]) -> str:
        """生成规范化的请求头"""
        result = []
        for key, value in headers.items():
            temp_str = str(quote(key.lower(), safe="")) + ":" + str(quote(value, safe=""))
            result.append(temp_str)
        result.sort()
        return "\n".join(result)

    def _generate_signature(self, method: str, uri: str, query: str, headers: Dict[str, str],
                            signed_headers: str, x_bce_date: str) -> str:
        """生成签名"""
        # 认证字符串前缀
        auth_string_prefix = f"bce-auth-v1/{self.ak}/{x_bce_date}/{self.expiration_seconds}"

        # 生成CanonicalRequest
        canonical_uri = quote(uri)
        canonical_query_string = query
        canonical_headers = self._generate_canonical_headers(headers)

        canonical_request = f"{method}\n{canonical_uri}\n{canonical_query_string}\n{canonical_headers}"

        # 生成signingKey
        signing_key = hmac.new(self.sk.encode('utf-8'), auth_string_prefix.encode('utf-8'), hashlib.sha256)

        # 生成Signature
        signature = hmac.new((signing_key.hexdigest()).encode('utf-8'),
                             canonical_request.encode('utf-8'),
                             hashlib.sha256)

        print(f"{auth_string_prefix}/{signed_headers}/{signature.hexdigest()}")
        # 生成Authorization
        return f"{auth_string_prefix}/{signed_headers}/{signature.hexdigest()}"

    def request(self, method: str, uri: str, query: str = "", body: Optional[Dict[str, Any]] = None,
                extra_headers: Optional[Dict[str, str]] = None) -> requests.Response:
        """
        发送API请求

        Args:
            method: 请求方法，如GET、POST等
            uri: 接口路径，如"/v2/batchinference"
            query: 查询字符串，如"Action=DescribeBatchInferenceTasks"
            body: 请求体，字典格式
            extra_headers: 额外的请求头

        Returns:
            requests.Response: 请求响应对象
        """
        # 生成x-bce-date
        x_bce_date = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

        # 构建基础header
        headers = {
            "Host": self.host,
            "content-type": "application/json;charset=utf-8",
            "x-bce-date": x_bce_date
        }

        # 添加额外的header
        if extra_headers:
            headers.update(extra_headers)

        # 签名使用的headers
        signed_headers = "content-type;host;x-bce-date"

        # 生成Authorization并添加到header
        headers['Authorization'] = self._generate_signature(
            method, uri, query, headers, signed_headers, x_bce_date
        )

        # 构建完整URL
        url = f"https://{self.host}{uri}"
        print(url)
        print(headers)
        if query:
            url = f"{url}?{query}"

        # 发送请求
        if body is None:
            body = {}

        return requests.post(url, headers=headers, data=json.dumps(body))

    def post(self, uri: str, query: str = "", body: Optional[Dict[str, Any]] = None,
             extra_headers: Optional[Dict[str, str]] = None) -> requests.Response:
        """POST请求封装"""
        return self.request("POST", uri, query, body, extra_headers)


# 使用示例
if __name__ == "__main__":
    # 创建API签名工具实例
    api_tool = BceApiSignatureTool(
        ak="YOUR_ACCESS_KEY",
        sk="YOUR_SECRET_KEY",
        host="qianfan.baidubce.com"
    )

    # 请求结构
    body = {}

    # 发送请求
    response = api_tool.post(
        uri="/v2/batchinference",
        query="Action=DescribeBatchInferenceTasks",
        body=body
    )

    print(json.dumps(response.json(), indent=True)) 