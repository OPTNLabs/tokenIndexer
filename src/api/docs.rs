use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::Json;
use serde_json::{json, Map, Value};

pub async fn openapi_json() -> Json<Value> {
    Json(openapi_spec())
}

pub async fn swagger_ui() -> impl IntoResponse {
    Html(
        r##"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>TokenIndex API Docs</title>
    <link rel="stylesheet" href="/swagger-ui-assets/swagger-ui.css" />
    <style>
      body { margin: 0; background: #0b1020; }
      .topbar { display: none; }
      .swagger-ui .info .title { color: #e5e7eb; }
      .swagger-ui .info p, .swagger-ui .opblock .opblock-summary-description { color: #cbd5e1; }
      .swagger-ui .scheme-container, .swagger-ui .opblock, .swagger-ui .models {
        border-color: #27324a;
      }
      .swagger-ui {
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }
    </style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="/swagger-ui-assets/swagger-ui-bundle.js"></script>
    <script>
      window.ui = SwaggerUIBundle({
        url: "/openapi.json",
        dom_id: "#swagger-ui",
        deepLinking: true,
        displayRequestDuration: true,
        persistAuthorization: true,
        docExpansion: "none",
        filter: true,
        presets: [SwaggerUIBundle.presets.apis],
        plugins: [SwaggerUIBundle.plugins.DownloadUrl],
        layout: "BaseLayout",
      });
    </script>
  </body>
</html>"##,
    )
}

pub async fn swagger_ui_css() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/css; charset=utf-8"),
        )],
        include_str!("swagger-ui.css"),
    )
}

pub async fn swagger_ui_bundle_js() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/javascript; charset=utf-8"),
        )],
        include_str!("swagger-ui-bundle.js"),
    )
}

fn openapi_spec() -> Value {
    json!({
        "openapi": "3.0.3",
        "info": {
            "title": "TokenIndex API",
            "version": "0.1.0",
            "description": "CashTokens-specific BCH indexer and REST API. Native integrations should prefer /v1 routes. Legacy BCMR-compatible routes remain available under /api."
        },
        "servers": [
            { "url": "/" }
        ],
        "tags": [
            { "name": "system", "description": "Health, metrics, and docs" },
            { "name": "native", "description": "Native TokenIndex routes under /v1" },
            { "name": "compatibility", "description": "Legacy BCMR-compatible routes under /api" }
        ],
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer"
                }
            },
            "parameters": {
                "CategoryPath": {
                    "name": "category",
                    "in": "path",
                    "required": true,
                    "schema": { "type": "string" },
                    "description": "32-byte hex CashTokens category"
                },
                "AddressPath": {
                    "name": "address",
                    "in": "path",
                    "required": true,
                    "schema": { "type": "string" },
                    "description": "Holder address"
                },
                "LegacyCategoryPath": {
                    "name": "category",
                    "in": "path",
                    "required": true,
                    "schema": { "type": "string" },
                    "description": "Legacy BCMR category"
                },
                "LegacyTypeKeyPath": {
                    "name": "type_key",
                    "in": "path",
                    "required": true,
                    "schema": { "type": "string" },
                    "description": "Legacy token type key"
                },
                "LegacyCommitmentPath": {
                    "name": "commitment",
                    "in": "path",
                    "required": true,
                    "schema": { "type": "string" },
                    "description": "Legacy NFT commitment"
                },
                "LegacyTxoPath": {
                    "name": "txo",
                    "in": "path",
                    "required": true,
                    "schema": { "type": "string" },
                    "description": "Legacy txid:vout identifier"
                },
                "LegacyIncludeIdentities": {
                    "name": "include_identities",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "string" },
                    "description": "Legacy registry flag accepted as a truthy string"
                },
                "LegacyIncludeTokenNfts": {
                    "name": "include_token_nfts",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "string" },
                    "description": "Legacy identity-snapshot flag accepted as a truthy string"
                },
                "LegacyLimit": {
                    "name": "limit",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "integer", "default": 10, "minimum": 1 },
                    "description": "Legacy pagination limit"
                },
                "LegacyOffset": {
                    "name": "offset",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "integer", "default": 0, "minimum": 0 },
                    "description": "Legacy pagination offset"
                },
                "LegacyPaginated": {
                    "name": "paginated",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "string" },
                    "description": "Legacy pagination flag accepted as a truthy string"
                },
                "LegacyPage": {
                    "name": "page",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "integer", "minimum": 1 },
                    "description": "Legacy page number"
                },
                "LegacyIncludeMetadata": {
                    "name": "include_metadata",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "string" },
                    "description": "Legacy cashtokens flag accepted as a truthy string"
                },
                "LegacyCapability": {
                    "name": "capability",
                    "in": "query",
                    "required": false,
                    "schema": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "style": "form",
                    "explode": true,
                    "description": "Repeatable cashtokens capability filter"
                },
                "KnownLimit": {
                    "name": "limit",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "integer", "default": 200, "minimum": 1, "maximum": 1000 },
                    "description": "Maximum number of known tokens to return"
                },
                "TopLimit": {
                    "name": "n",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "integer", "default": 50, "minimum": 1, "maximum": 500 },
                    "description": "Maximum number of top holders to return"
                },
                "PageLimit": {
                    "name": "limit",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "integer", "default": 100, "minimum": 1, "maximum": 500 },
                    "description": "Maximum number of rows to return"
                },
                "MempoolTopLimit": {
                    "name": "n",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "integer", "default": 20, "minimum": 1, "maximum": 200 },
                    "description": "Maximum number of mempool deltas to return"
                },
                "Cursor": {
                    "name": "cursor",
                    "in": "query",
                    "required": false,
                    "schema": { "type": "string" },
                    "description": "Base64url-encoded pagination cursor"
                }
            },
            "responses": {
                "BadRequest": {
                    "description": "Invalid category, address, cursor, or limit"
                },
                "Unauthorized": {
                    "description": "Missing or invalid bearer token"
                },
                "Forbidden": {
                    "description": "Request IP is not allowed"
                },
                "NotFound": {
                    "description": "Requested resource was not found"
                },
                "TooManyRequests": {
                    "description": "Rate limited"
                },
                "ServerError": {
                    "description": "Internal server error"
                }
            },
            "schemas": {
                "LegacyLatestBlockResponse": {
                    "type": "object",
                    "required": ["height"],
                    "properties": {
                        "height": { "type": "integer", "format": "int64" }
                    },
                    "additionalProperties": false
                },
                "LegacyTokenPayloadResponse": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "description": { "type": "string" },
                        "tags": { "type": "object", "additionalProperties": true },
                        "migrated": { "type": "boolean" },
                        "status": { "type": "string" },
                        "splitId": { "type": "string" },
                        "uris": { "type": "object", "additionalProperties": true },
                        "extensions": { "type": "object", "additionalProperties": true },
                        "token": {
                            "type": "object",
                            "properties": {
                                "category": { "type": "string" },
                                "symbol": { "type": "string" },
                                "decimals": { "type": "integer" },
                                "nfts": { "type": "object", "additionalProperties": true }
                            },
                            "additionalProperties": true
                        },
                        "type_metadata": { "type": "object", "additionalProperties": true },
                        "is_nft": { "type": "boolean" },
                        "nft_type": { "type": "string" }
                    },
                    "additionalProperties": true
                },
                "LegacyRegistryMeta": {
                    "type": "object",
                    "required": ["registry_id", "category"],
                    "properties": {
                        "registry_id": { "type": "integer", "format": "int64" },
                        "category": { "type": "string" },
                        "authbase": { "type": "string" },
                        "identity_history": { "type": "string", "format": "date-time" }
                    },
                    "additionalProperties": false
                },
                "LegacyRegistryContentsResponse": {
                    "type": "object",
                    "additionalProperties": true
                },
                "LegacyIdentitySnapshotResponse": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "description": { "type": "string" },
                        "tags": { "type": "object", "additionalProperties": true },
                        "migrated": { "type": "boolean" },
                        "status": { "type": "string" },
                        "splitId": { "type": "string" },
                        "uris": { "type": "object", "additionalProperties": true },
                        "extensions": { "type": "object", "additionalProperties": true },
                        "token": { "type": "object", "additionalProperties": true },
                        "_meta": { "$ref": "#/components/schemas/LegacyRegistryMeta" }
                    },
                    "additionalProperties": true
                },
                "LegacyTokenCategoryResponse": {
                    "type": "object",
                    "properties": {
                        "token": {
                            "type": "object",
                            "properties": {
                                "category": { "type": "string" },
                                "symbol": { "type": "string" },
                                "decimals": { "type": "integer" }
                            },
                            "additionalProperties": true
                        },
                        "_meta": { "$ref": "#/components/schemas/LegacyRegistryMeta" }
                    },
                    "additionalProperties": true
                },
                "LegacyNftMeta": {
                    "allOf": [
                        { "$ref": "#/components/schemas/LegacyRegistryMeta" },
                        {
                            "type": "object",
                            "properties": {
                                "commitment": { "type": "string" }
                            },
                            "additionalProperties": false
                        }
                    ]
                },
                "LegacyNftTypeResponse": {
                    "type": "object",
                    "properties": {
                        "_meta": { "$ref": "#/components/schemas/LegacyNftMeta" }
                    },
                    "additionalProperties": true
                },
                "LegacyRegistryNftsResponse": {
                    "type": "object",
                    "properties": {
                        "nfts": {
                            "type": "array",
                            "items": { "type": "object", "additionalProperties": true }
                        },
                        "_meta": { "$ref": "#/components/schemas/LegacyRegistryMeta" }
                    },
                    "additionalProperties": true
                },
                "LegacyNftTypesPageResponse": {
                    "type": "object",
                    "properties": {
                        "count": { "type": "integer", "format": "int64" },
                        "limit": { "type": "integer" },
                        "offset": { "type": "integer" },
                        "previous": { "type": "string", "nullable": true },
                        "next": { "type": "string", "nullable": true },
                        "results": {
                            "type": "array",
                            "items": { "type": "object", "additionalProperties": true }
                        }
                    },
                    "additionalProperties": false
                },
                "LegacyCashtokenEntry": {
                    "type": "object",
                    "properties": {
                        "category": { "type": "string" },
                        "commitment": { "type": "string", "nullable": true },
                        "capability": { "type": "string", "nullable": true },
                        "amount": { "type": "string" },
                        "metadata": {
                            "anyOf": [
                                { "type": "string" },
                                { "type": "object", "additionalProperties": true }
                            ]
                        }
                    },
                    "additionalProperties": false
                },
                "LegacyCashtokensPageResponse": {
                    "type": "object",
                    "properties": {
                        "count": { "type": "integer", "format": "int64" },
                        "next": { "type": "string", "nullable": true },
                        "previous": { "type": "string", "nullable": true },
                        "capability": {
                            "type": "array",
                            "items": { "type": "string" }
                        },
                        "results": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/LegacyCashtokenEntry" }
                        }
                    },
                    "additionalProperties": false
                },
                "LegacyAuthchainHeadResponse": {
                    "type": "object",
                    "properties": {
                        "authchain_head": {
                            "type": "object",
                            "required": ["txid", "owner"],
                            "properties": {
                                "txid": { "type": "string" },
                                "owner": {
                                    "anyOf": [
                                        { "type": "string" },
                                        { "type": "object", "additionalProperties": true }
                                    ],
                                    "nullable": true
                                }
                            },
                            "additionalProperties": false
                        }
                    },
                    "additionalProperties": false
                },
                "LegacyPublishedUrlResponse": {
                    "type": "object",
                    "properties": {
                        "url": { "type": "string" }
                    },
                    "additionalProperties": false
                },
                "LegacyReindexResponse": {
                    "type": "object",
                    "properties": {
                        "success": { "type": "string" }
                    },
                    "additionalProperties": false
                },
            }
        },
        "paths": {
            "/health": {
                "get": simple_operation(
                    "Health check",
                    "Reports whether the service is up and responding.",
                    "system",
                    vec![],
                    vec![("200", "Service is healthy")],
                    false
                )
            },
            "/health/details": {
                "get": simple_operation(
                    "Health details",
                    "Returns chain and database diagnostics for the configured stack.",
                    "system",
                    vec![],
                    vec![("200", "Health diagnostic payload")],
                    false
                )
            },
            "/metrics": {
                "get": simple_operation(
                    "Prometheus metrics",
                    "Returns Prometheus-compatible metrics for the service.",
                    "system",
                    vec![],
                    vec![("200", "Metrics payload")],
                    false
                )
            },
            "/openapi.json": {
                "get": simple_operation(
                    "OpenAPI document",
                    "Returns the OpenAPI 3 document consumed by Swagger UI.",
                    "system",
                    vec![],
                    vec![("200", "OpenAPI JSON")],
                    false
                )
            },
            "/docs": {
                "get": simple_operation(
                    "Swagger UI",
                    "Interactive API documentation for TokenIndex.",
                    "system",
                    vec![],
                    vec![("200", "Swagger UI HTML")],
                    false
                )
            },
            "/v1/tokens/known": {
                "get": simple_operation(
                    "Known tokens",
                    "Returns the highest-activity indexed tokens.",
                    "native",
                    vec![param_ref("KnownLimit")],
                    vec![("200", "Token discovery list"), ("400", "Invalid query"), ("401", "Unauthorized"), ("403", "Forbidden"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/token/{category}": {
                "get": simple_operation(
                    "Token summary",
                    "Returns the unified token summary with BCMR and authchain fields when available.",
                    "native",
                    vec![param_ref("CategoryPath")],
                    vec![("200", "Token summary"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Token not indexed"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/token/{category}/summary": {
                "get": simple_operation(
                    "Token summary alias",
                    "Alias for GET /v1/token/{category}.",
                    "native",
                    vec![param_ref("CategoryPath")],
                    vec![("200", "Token summary"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Token not indexed"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/token/{category}/bcmr": {
                "get": simple_operation(
                    "BCMR metadata alias",
                    "Returns BCMR metadata for a token category.",
                    "native",
                    vec![param_ref("CategoryPath")],
                    vec![("200", "BCMR metadata"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "BCMR metadata not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/bcmr/{category}": {
                "get": simple_operation(
                    "BCMR metadata",
                    "Returns the resolved BCMR registry metadata for a category.",
                    "native",
                    vec![param_ref("CategoryPath")],
                    vec![("200", "BCMR metadata"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "BCMR metadata not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/token/{category}/authchain/head": {
                "get": simple_operation(
                    "Authchain head",
                    "Returns the authchain head transaction and owner for a token category.",
                    "native",
                    vec![param_ref("CategoryPath")],
                    vec![("200", "Authchain head"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Category not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/token/{category}/holders/top": {
                "get": simple_operation(
                    "Top holders",
                    "Returns the highest-balance holders for the token category.",
                    "native",
                    vec![param_ref("CategoryPath"), param_ref("TopLimit")],
                    vec![("200", "Top holders"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/token/{category}/holders": {
                "get": simple_operation(
                    "Paged holders",
                    "Returns a cursor-paginated holder list for the token category.",
                    "native",
                    vec![param_ref("CategoryPath"), param_ref("PageLimit"), param_ref("Cursor")],
                    vec![("200", "Paged holders"), ("400", "Invalid category or cursor"), ("401", "Unauthorized"), ("403", "Forbidden"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/token/{category}/nfts": {
                "get": simple_operation(
                    "NFT inventory",
                    "Returns a cursor-paginated list of unspent NFT UTXOs in the token category.",
                    "native",
                    vec![param_ref("CategoryPath"), param_ref("PageLimit"), param_ref("Cursor")],
                    vec![("200", "NFT inventory"), ("400", "Invalid category or cursor"), ("401", "Unauthorized"), ("403", "Forbidden"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/token/{category}/holder/{address}": {
                "get": simple_operation(
                    "Holder eligibility",
                    "Returns holder eligibility and effective balance fields for a token category and address.",
                    "native",
                    vec![param_ref("CategoryPath"), param_ref("AddressPath")],
                    vec![("200", "Holder eligibility"), ("400", "Invalid category or address"), ("401", "Unauthorized"), ("403", "Forbidden"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/address/{address}/tokens": {
                "get": simple_operation(
                    "Address tokens",
                    "Returns all token balances associated with the address.",
                    "native",
                    vec![param_ref("AddressPath"), param_ref("PageLimit")],
                    vec![("200", "Address token inventory"), ("400", "Invalid address or limit"), ("401", "Unauthorized"), ("403", "Forbidden"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/token/{category}/mempool": {
                "get": simple_operation(
                    "Mempool overlay",
                    "Returns the mempool overlay for a token category.",
                    "native",
                    vec![param_ref("CategoryPath"), param_ref("MempoolTopLimit")],
                    vec![("200", "Mempool overlay"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/v1/token/{category}/insights": {
                "get": simple_operation(
                    "Token insights",
                    "Returns token concentration, activity, and mempool overlay aggregates.",
                    "native",
                    vec![param_ref("CategoryPath")],
                    vec![("200", "Insights payload"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Token not indexed"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/status/latest-block/": {
                "get": json_operation(
                    "Legacy latest block",
                    "Returns the latest indexed block height for BCMR compatibility clients.",
                    "compatibility",
                    vec![],
                    "200",
                    "Latest block height",
                    schema_ref("LegacyLatestBlockResponse"),
                    vec![("200", "Latest block height"), ("500", "Server error")],
                    true
                )
            },
            "/api/tokens/{category}/": {
                "get": json_operation(
                    "Legacy token summary",
                    "Returns the legacy BCMR token payload for a category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "Legacy token payload",
                    schema_ref("LegacyTokenPayloadResponse"),
                    vec![("200", "Legacy token payload"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Token not indexed"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/tokens/{category}/icon-symbol": {
                "get": json_operation(
                    "Legacy icon and symbol",
                    "Returns the BCMR icon URI and symbol for a category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "Legacy icon-symbol payload",
                    json!({
                        "type": "object",
                        "properties": {
                            "category": { "type": "string" },
                            "icon_uri": { "type": "string", "nullable": true },
                            "symbol": { "type": "string", "nullable": true }
                        },
                        "additionalProperties": false
                    }),
                    vec![("200", "Legacy icon-symbol payload"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Registry not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/tokens/{category}/{type_key}/": {
                "get": json_operation(
                    "Legacy token type",
                    "Returns a legacy token-type specific payload for the category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath"), param_ref("LegacyTypeKeyPath")],
                    "200",
                    "Legacy token type payload",
                    schema_ref("LegacyTokenPayloadResponse"),
                    vec![("200", "Legacy token type payload"), ("400", "Invalid category or type key"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Token not indexed"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/registries/{category}/latest/": {
                "get": json_operation(
                    "Legacy latest registry",
                    "Returns the latest legacy registry contents for a category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "Registry contents",
                    schema_ref("LegacyRegistryContentsResponse"),
                    vec![("200", "Registry contents"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Registry not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/registries/{txo}/": {
                "get": json_operation(
                    "Legacy registry by txo",
                    "Returns registry contents for a legacy txid:vout lookup.",
                    "compatibility",
                    vec![param_ref("LegacyTxoPath")],
                    "200",
                    "Registry contents",
                    schema_ref("LegacyRegistryContentsResponse"),
                    vec![("200", "Registry contents"), ("422", "Invalid txo"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Registry not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/bcmr/{category}/": {
                "get": json_operation(
                    "Legacy BCMR contents",
                    "Returns the BCMR registry document for a category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "BCMR document",
                    schema_ref("LegacyRegistryContentsResponse"),
                    vec![("200", "BCMR document"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Registry not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/bcmr/{category}/token/": {
                "get": json_operation(
                    "Legacy BCMR token",
                    "Returns the token object from the BCMR registry payload.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "BCMR token payload",
                    schema_ref("LegacyTokenCategoryResponse"),
                    vec![("200", "BCMR token payload"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Registry not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/bcmr/{category}/token/nfts/{commitment}/": {
                "get": json_operation(
                    "Legacy BCMR NFT",
                    "Returns the legacy BCMR NFT payload for a category and commitment.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath"), param_ref("LegacyCommitmentPath")],
                    "200",
                    "BCMR NFT payload",
                    schema_ref("LegacyNftTypeResponse"),
                    vec![("200", "BCMR NFT payload"), ("400", "Invalid category or commitment"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Token or NFT not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/bcmr/{category}/uris/": {
                "get": json_operation(
                    "Legacy BCMR URIs",
                    "Returns the legacy BCMR URI map for a category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "BCMR URI payload",
                    json!({"type":"object","additionalProperties":true}),
                    vec![("200", "BCMR URI payload"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Registry not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/bcmr/{category}/uris/icon": {
                "get": json_operation(
                    "Legacy BCMR icon URI",
                    "Returns the BCMR icon URI for a category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "Icon URI",
                    json!({"type":"string","nullable":true}),
                    vec![("200", "Icon URI"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Icon URI not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/bcmr/{category}/uris/published-url": {
                "get": json_operation(
                    "Legacy BCMR published URL",
                    "Returns the BCMR source URL recorded for the category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "Published URL",
                    schema_ref("LegacyPublishedUrlResponse"),
                    vec![("200", "Published URL"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "Category not found"), ("429", "Too many requests"), ("500", "Server error")],
                    true
                )
            },
            "/api/bcmr/{category}/reindex/": {
                "get": json_operation(
                    "Legacy BCMR reindex",
                    "Queues a BCMR reindex request for the category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "Reindex queued",
                    schema_ref("LegacyReindexResponse"),
                    vec![("200", "Reindex queued"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/authchain/{category}/head/": {
                "get": json_operation(
                    "Legacy authchain head",
                    "Returns the authchain head for a category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "Authchain head",
                    schema_ref("LegacyAuthchainHeadResponse"),
                    vec![("200", "Authchain head"), ("400", "Invalid category"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/registry/{category}/": {
                "get": json_operation(
                    "Legacy registry summary",
                    "Returns the legacy registry summary for a category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath"), param_ref("LegacyIncludeIdentities")],
                    "200",
                    "Registry summary",
                    schema_ref("LegacyRegistryContentsResponse"),
                    vec![("200", "Registry summary"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/registry/{category}/identity-snapshot/": {
                "get": json_operation(
                    "Legacy identity snapshot",
                    "Returns the legacy identity snapshot for a category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath"), param_ref("LegacyIncludeTokenNfts")],
                    "200",
                    "Identity snapshot",
                    schema_ref("LegacyIdentitySnapshotResponse"),
                    vec![("200", "Identity snapshot"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/registry/{category}/identity-snapshot/token-category/": {
                "get": json_operation(
                    "Legacy token category",
                    "Returns the token-category projection inside the legacy registry snapshot.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "Token category projection",
                    schema_ref("LegacyTokenCategoryResponse"),
                    vec![("200", "Token category projection"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/registry/{category}/identity-snapshot/token-category/nfts/": {
                "get": json_operation(
                    "Legacy registry NFTs",
                    "Returns the legacy NFT projection from the registry snapshot.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "NFT projection",
                    schema_ref("LegacyRegistryNftsResponse"),
                    vec![("200", "NFT projection"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/registry/{category}/identity-snapshot/token-category/nfts/parse/bytecode/": {
                "get": json_operation(
                    "Legacy NFT parse bytecode",
                    "Returns the parsed bytecode payload from the legacy registry snapshot.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath")],
                    "200",
                    "Parse bytecode payload",
                    json!({
                        "type": "object",
                        "properties": {
                            "bytecode": {
                                "anyOf": [
                                    { "type": "string" },
                                    { "type": "object", "additionalProperties": true }
                                ],
                                "nullable": true
                            },
                            "_meta": { "$ref": "#/components/schemas/LegacyNftMeta" }
                        },
                        "additionalProperties": true
                    }),
                    vec![("200", "Parse bytecode payload"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/registry/{category}/identity-snapshot/token-category/nfts/parse/types/": {
                "get": json_operation(
                    "Legacy NFT types",
                    "Returns the NFT type map with optional legacy pagination parameters.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath"), param_ref("LegacyLimit"), param_ref("LegacyOffset"), param_ref("LegacyPaginated")],
                    "200",
                    "NFT types",
                    json!({
                        "oneOf": [
                            { "type": "array", "items": { "type": "object", "additionalProperties": true } },
                            { "$ref": "#/components/schemas/LegacyNftTypesPageResponse" }
                        ]
                    }),
                    vec![("200", "NFT types"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/registry/{category}/identity-snapshot/token-category/nfts/parse/types/{commitment}/": {
                "get": json_operation(
                    "Legacy NFT type",
                    "Returns one legacy NFT type entry by commitment.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath"), param_ref("LegacyCommitmentPath")],
                    "200",
                    "NFT type entry",
                    schema_ref("LegacyNftTypeResponse"),
                    vec![("200", "NFT type entry"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/cashtokens/": {
                "get": json_operation(
                    "Legacy cashtokens",
                    "Returns the legacy cashtokens view.",
                    "compatibility",
                    vec![param_ref("LegacyPage"), param_ref("LegacyIncludeMetadata"), param_ref("LegacyCapability")],
                    "200",
                    "Cashtokens payload",
                    schema_ref("LegacyCashtokensPageResponse"),
                    vec![("200", "Cashtokens payload"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/cashtokens/{category}/": {
                "get": json_operation(
                    "Legacy cashtokens category",
                    "Returns the legacy cashtokens view for a category.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath"), param_ref("LegacyPage"), param_ref("LegacyIncludeMetadata"), param_ref("LegacyCapability")],
                    "200",
                    "Cashtokens payload",
                    schema_ref("LegacyCashtokensPageResponse"),
                    vec![("200", "Cashtokens payload"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/cashtokens/{category}/{token_type}/": {
                "get": json_operation(
                    "Legacy cashtokens token type",
                    "Returns the legacy cashtokens view for a category and token type.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath"), param_ref("LegacyTypeKeyPath"), param_ref("LegacyPage"), param_ref("LegacyIncludeMetadata"), param_ref("LegacyCapability")],
                    "200",
                    "Cashtokens payload",
                    schema_ref("LegacyCashtokensPageResponse"),
                    vec![("200", "Cashtokens payload"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            },
            "/api/cashtokens/{category}/{token_type}/{commitment}/": {
                "get": json_operation(
                    "Legacy cashtokens commitment",
                    "Returns the legacy cashtokens view for a category, token type, and commitment.",
                    "compatibility",
                    vec![param_ref("LegacyCategoryPath"), param_ref("LegacyTypeKeyPath"), param_ref("LegacyCommitmentPath"), param_ref("LegacyPage"), param_ref("LegacyIncludeMetadata"), param_ref("LegacyCapability")],
                    "200",
                    "Cashtokens payload",
                    schema_ref("LegacyCashtokensPageResponse"),
                    vec![("200", "Cashtokens payload"), ("401", "Unauthorized"), ("403", "Forbidden"), ("500", "Server error")],
                    true
                )
            }
        }
    })
}

fn simple_operation(
    summary: &str,
    description: &str,
    tag: &str,
    parameters: Vec<Value>,
    responses: Vec<(&str, &str)>,
    secured: bool,
) -> Value {
    let mut response_map = Map::new();
    for (status, description) in responses {
        response_map.insert(
            status.to_string(),
            json!({
                "description": description
            }),
        );
    }

    let mut op = json!({
        "summary": summary,
        "description": description,
        "tags": [tag],
        "parameters": parameters,
        "responses": response_map
    });

    if secured {
        op["security"] = json!([{ "bearerAuth": [] }]);
    }

    op
}

fn json_operation(
    summary: &str,
    description: &str,
    tag: &str,
    parameters: Vec<Value>,
    success_status: &str,
    success_description: &str,
    success_schema: Value,
    responses: Vec<(&str, &str)>,
    secured: bool,
) -> Value {
    let mut response_map = Map::new();
    response_map.insert(
        success_status.to_string(),
        json!({
            "description": success_description,
            "content": {
                "application/json": {
                    "schema": success_schema
                }
            }
        }),
    );
    for (status, description) in responses {
        response_map.insert(
            status.to_string(),
            json!({
                "description": description
            }),
        );
    }

    let mut op = json!({
        "summary": summary,
        "description": description,
        "tags": [tag],
        "parameters": parameters,
        "responses": response_map
    });

    if secured {
        op["security"] = json!([{ "bearerAuth": [] }]);
    }

    op
}

fn param_ref(name: &str) -> Value {
    match name {
        "CategoryPath" => json!({"$ref": "#/components/parameters/CategoryPath"}),
        "AddressPath" => json!({"$ref": "#/components/parameters/AddressPath"}),
        "LegacyCategoryPath" => json!({"$ref": "#/components/parameters/LegacyCategoryPath"}),
        "LegacyTypeKeyPath" => json!({"$ref": "#/components/parameters/LegacyTypeKeyPath"}),
        "LegacyCommitmentPath" => json!({"$ref": "#/components/parameters/LegacyCommitmentPath"}),
        "LegacyTxoPath" => json!({"$ref": "#/components/parameters/LegacyTxoPath"}),
        "LegacyIncludeIdentities" => {
            json!({"$ref": "#/components/parameters/LegacyIncludeIdentities"})
        }
        "LegacyIncludeTokenNfts" => {
            json!({"$ref": "#/components/parameters/LegacyIncludeTokenNfts"})
        }
        "LegacyLimit" => json!({"$ref": "#/components/parameters/LegacyLimit"}),
        "LegacyOffset" => json!({"$ref": "#/components/parameters/LegacyOffset"}),
        "LegacyPaginated" => json!({"$ref": "#/components/parameters/LegacyPaginated"}),
        "LegacyPage" => json!({"$ref": "#/components/parameters/LegacyPage"}),
        "LegacyIncludeMetadata" => json!({"$ref": "#/components/parameters/LegacyIncludeMetadata"}),
        "KnownLimit" => json!({"$ref": "#/components/parameters/KnownLimit"}),
        "TopLimit" => json!({"$ref": "#/components/parameters/TopLimit"}),
        "PageLimit" => json!({"$ref": "#/components/parameters/PageLimit"}),
        "MempoolTopLimit" => json!({"$ref": "#/components/parameters/MempoolTopLimit"}),
        "Cursor" => json!({"$ref": "#/components/parameters/Cursor"}),
        _ => Value::Null,
    }
}

fn schema_ref(name: &str) -> Value {
    json!({ "$ref": format!("#/components/schemas/{name}") })
}
