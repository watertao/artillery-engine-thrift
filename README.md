# artillery-engine-thrift

Artillery engine for Apache Thrift RPC load testing. This engine allows you to use [Artillery](https://www.artillery.io/) to perform load testing on Thrift-based services.

## Installation

```bash
npm install artillery-engine-thrift --save-dev
```

## Requirements

- Node.js >= 14.0.0
- Artillery >= 2.0.0

## Usage

### 1. Install Artillery

```bash
npm install artillery --save-dev
```

### 2. Generate Thrift Service Code

First, generate your Thrift service JavaScript code:

```bash
thrift --gen js:node your-service.thrift
```

### 3. Configure Artillery

Create an Artillery configuration file (e.g., `artillery-config.yml`):

```yaml
config:
  target: "thrift://localhost:8080"
  engines:
    thrift:
      services:
        YourServiceName:
          module: "./gen-nodejs/YourService.js"  # Path to generated Thrift service module
          qName: "com.example.YourService"        # Fully qualified service name
          host: "localhost"                       # Thrift server host
          port: 8080                               # Thrift server port
          transport: "TFramedTransport"            # Transport: TBufferedTransport, TFramedTransport
          protocol: "TBinaryProtocol"             # Protocol: TBinaryProtocol, TCompactProtocol, TJSONProtocol
          connectTimeout: 5000                     # Connection timeout in ms (default: 5000)
          timeout: 10000                           # Request timeout in ms (default: 10000)
          maxAttempts: 3                           # Max retry attempts (default: 3)
          retryMaxDelay: 1000                       # Max retry delay in ms (default: 1000)

scenarios:
  - name: "Thrift Load Test"
    engine: "thrift"
    flow:
      - thrift:
          service: "YourServiceName"
          method: "yourMethod"
          args: [{ param1: "{{ $randomString() }}" }]
          capture:
            resultVar: "$.response.field"
          match:
            "$.status": "success"
```

### 4. Run the Test

```bash
artillery run artillery-config.yml
```

## Configuration Options

### Service Configuration

- **module**: Path to the generated Thrift service JavaScript file
- **qName**: Fully qualified service name (e.g., `com.example.ServiceName`)
- **host**: Thrift server hostname or IP address
- **port**: Thrift server port number
- **transport**: Transport type (`TBufferedTransport` or `TFramedTransport`)
- **protocol**: Protocol type (`TBinaryProtocol`, `TCompactProtocol`, or `TJSONProtocol`)
- **connectTimeout**: Connection timeout in milliseconds (default: 5000)
- **timeout**: Request timeout in milliseconds (default: 10000)
- **maxAttempts**: Maximum retry attempts (default: 3)
- **retryMaxDelay**: Maximum retry delay in milliseconds (default: 1000)

### Scenario Step Options

- **service**: Service name (must match the key in `engines.thrift.services`)
- **method**: Method name to call
- **args**: Array of arguments to pass to the method (supports Artillery template variables)
- **capture**: Capture response data to variables using JSONPath
- **match**: Validate response values using JSONPath
- **continueOnError**: Continue scenario execution on error (default: false)

## Features

- ✅ Support for multiple Thrift services
- ✅ Connection pooling and client caching
- ✅ Automatic retry with configurable attempts
- ✅ Support for Artillery template variables in arguments
- ✅ Response data capture using JSONPath
- ✅ Response validation using JSONPath
- ✅ Custom metrics tracking
- ✅ Multiple transport and protocol options

## Example

See the [examples directory](./examples) for complete working examples.

## License

MPL-2.0

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

