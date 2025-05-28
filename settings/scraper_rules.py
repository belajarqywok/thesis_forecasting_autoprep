from typing import List, Dict, Any

class ScraperRules:
  # Main
  SCRAPER_RATE_LIMIT_HANDLE: int = 1
  SCRAPER_THREAD_WORKER:     int = 25

  # Retry mechanism
  SCRAPER_MAXIMUM_RETRY:     int = 10
  SCRAPER_EXPONENTIAL_RETRY: int = 1

  # Browser Agents
  # Documentation: https://curl-cffi.readthedocs.io/en/latest/impersonate.html
  SCRAPER_BROWSER_AGENTS: List[str] = [
    # Google Chrome Browser
    'chrome',    'chrome99',   'chrome100',
    'chrome101', 'chrome104',  'chrome107',
    'chrome110', 'chrome116',  'chrome119',
    'chrome120', 'chrome123',  'chrome124',
    'chrome131', 'chrome99_android', 'chrome131_android',

    # Microsoft Edge Browser
    'edge99', 'edge101',

    # Safari Browser
    'safari17_0', 'safari17_2_ios',
    'safari18_0', 'safari18_0_ios',

    # Firefox Browser
    'firefox133'
  ]

  # Fingerprint (NOTES ONLY, wkwk...)
  # Documentation: https://curl-cffi.readthedocs.io/en/latest/impersonate.html
  OKHTTP4_ANDROID10_JA3: str = ",".join(
    [
      "771",
      "4865-4866-4867-49195-49196-52393-49199-49200-52392-49171-49172-156-157-47-53",
      "0-23-65281-10-11-35-16-5-13-51-45-43-21",
      "29-23-24",
      "0",
    ]
  )

  OKHTTP4_ANDROID10_AKAMAI: str = "4:16777216|16711681|0|m,p,a,s"

  # Documentation:
  # https://curl-cffi.readthedocs.io/en/latest/impersonate.html#how-to-use-my-own-fingerprints-other-than-the-builtin-ones-e-g-okhttp
  EXTRA_FP: Dict[str, Any] = {
    "tls_signature_algorithms": [
      "ecdsa_secp256r1_sha256",
      "rsa_pss_rsae_sha256",
      "rsa_pkcs1_sha256",
      "ecdsa_secp384r1_sha384",
      "rsa_pss_rsae_sha384",
      "rsa_pkcs1_sha384",
      "rsa_pss_rsae_sha512",
      "rsa_pkcs1_sha512",
      "rsa_pkcs1_sha1",
    ]
    # other options:
    # tls_min_version: int = CurlSslVersion.TLSv1_2
    # tls_grease: bool = False
    # tls_permute_extensions: bool = False
    # tls_cert_compression: Literal["zlib", "brotli"] = "brotli"
    # tls_signature_algorithms: Optional[List[str]] = None
    # http2_stream_weight: int = 256
    # http2_stream_exclusive: int = 1
  }

