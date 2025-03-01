"""
Subgraph query.
"""

UNISWAP_V3_QUERY = """query MyQuery {{
  swaps(
    first: 1000
    orderBy: timestamp
    orderDirection: asc
    where: {{timestamp_gte: "{ts}"}}
  ) {{
    id
    transaction {{
      id
    }}
    timestamp
    pool {{
      id
      token0 {{
        id
        symbol
      }}
      token1 {{
        id
        symbol
      }}
    }}
    amount0
    amount1
    amountUSD
    origin
    recipient
    sender
  }}
}}"""
