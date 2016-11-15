package httpinterface
const SwaggerJSON = `{
  "swagger": "2.0",
  "info": {
    "title": "Version 4.0",
    "version": "version not set"
  },
  "schemes": [
    "http",
    "https"
  ],
  "consumes": [
    "application/json"
  ],
  "produces": [
    "application/json"
  ],
  "paths": {
    "/v4.0/raw": {
      "post": {
        "operationId": "RawValues",
        "responses": {
          "200": {
            "description": "(streaming responses)",
            "schema": {
              "$ref": "#/definitions/grpcinterfaceRawValuesResponse"
            }
          }
        },
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/grpcinterfaceRawValuesParams"
            }
          }
        ],
        "tags": [
          "BTrDB"
        ]
      }
    }
  },
  "definitions": {
    "grpcinterfaceMash": {
      "type": "object"
    },
    "grpcinterfaceRawPoint": {
      "type": "object",
      "properties": {
        "time": {
          "type": "string",
          "format": "int64"
        },
        "value": {
          "type": "number",
          "format": "double"
        }
      }
    },
    "grpcinterfaceRawValuesParams": {
      "type": "object",
      "properties": {
        "end": {
          "type": "string",
          "format": "int64"
        },
        "start": {
          "type": "string",
          "format": "int64"
        },
        "uuid": {
          "type": "string",
          "format": "byte"
        },
        "versionMajor": {
          "type": "string",
          "format": "uint64"
        }
      }
    },
    "grpcinterfaceRawValuesResponse": {
      "type": "object",
      "properties": {
        "stat": {
          "$ref": "#/definitions/grpcinterfaceStatus"
        },
        "values": {
          "type": "array",
          "items": {
            "$ref": "#/definitions/grpcinterfaceRawPoint"
          }
        },
        "versionMajor": {
          "type": "string",
          "format": "uint64"
        },
        "versionMinor": {
          "type": "string",
          "format": "uint64"
        }
      }
    },
    "grpcinterfaceStatus": {
      "type": "object",
      "properties": {
        "code": {
          "type": "integer",
          "format": "int64"
        },
        "mash": {
          "$ref": "#/definitions/grpcinterfaceMash"
        },
        "msg": {
          "type": "string",
          "format": "string"
        }
      }
    }
  }
}
`;