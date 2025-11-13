const A = require("async");
const thrift = require("thrift");
const debug = require("debug")("engine:thrift");

/**
 * Artillery engine for Apache Thrift RPC load testing
 *
 * Engine Configuration:
 * engines:
 *   thrift:
 *     services:
 *       ServiceName:
 *         module: "./gen-nodejs/ServiceName.js"  # Path to generated Thrift service module
 *         qName: "com.example.ServiceName"       # Fully qualified service name
 *         host: "localhost"                      # Thrift server host
 *         port: 8080                             # Thrift server port
 *         transport: "TFramedTransport"          # Transport: TBufferedTransport, TFramedTransport
 *         protocol: "TBinaryProtocol"            # Protocol: TBinaryProtocol, TCompactProtocol, TJSONProtocol
 *         connectTimeout: 5000                   # Connection timeout in ms (default: 5000)
 *         timeout: 10000                         # Request timeout in ms (default: 10000)
 *         maxAttempts: 3                         # Max retry attempts (default: 3)
 *         retryMaxDelay: 1000                    # Max retry delay in ms (default: 1000)
 *
 * Scenario Step Example:
 * - thrift:
 *     service: "ServiceName"
 *     method: "methodName"
 *     args: [{ param1: "{{ $randomString() }}" }]  # Supports Artillery template variables
 *     capture:
 *       resultVar: "$.response.field"              # Capture response data to variables
 *     match:
 *       "$.status": "success"                      # Validate response values
 *     continueOnError: false                       # Continue on error (default: false)
 */
class ThriftEngine {
  /**
   * Artillery engine constructor
   * this method will be called 1 time
   * @param {Object} script - Complete script object containing config and scenarios properties
   * @param {EventEmitter} ee - Event emitter for subscribing to Artillery events and reporting custom metrics
   * @param {Object} helpers - Collection of utility functions provided by Artillery
   */
  constructor(script, ee, helpers) {
    debug(process.cwd());
    this.script = script;
    this.ee = ee;
    this.helpers = helpers;

    // Get target address
    this.target = script.config.target;

    // Get Thrift engine configuration
    this.engineConf = { ...script.config.engines?.thrift };

    // Validate configuration
    this._validateConfig();

    // Connection pool and client cache
    this.connections = new Map();
    this.clients = new Map();

    // Load service definitions
    this.serviceClasses = {};
    if (this.engineConf?.services) {
      this._loadServices();
    }

    // Initialize custom metrics tracking
    this.metrics = {
      requestCounts: new Map(),
      responseTimes: new Map(),
      errorCounts: new Map(),
      startTime: Date.now()
    };

    // Cleanup function - Artillery will call this when test ends
    this.ee.on("done", () => {
      console.log("clean resources....")
      this.cleanup();
    });

    return this;
  }

  /**
   * Create VU (Virtual User) function for each scenario
   * call for each VU's each scenario
   *
   * @param {Object} scenarioSpec - Scenario specification
   * @param {EventEmitter} ee - Scenario-level event emitter
   */
  createScenario(scenarioSpec, ee) {
    // Convert each step in flow to executable function
    const tasks = scenarioSpec.flow.map((rs) => this._step(rs, ee));

    // Return scenario execution function
    return function scenario(initialContext, callback) {
      // Notify Artillery that scenario started
      ee.emit("started");

      // VU initialization function
      function vuInit(callback) {
        // Can add VU-specific initialization code here
        // Such as setting authentication info, session ID, etc.
        initialContext.vars = initialContext.vars || {};
        return callback(null, initialContext);
      }

      // Combine initialization and all steps
      const steps = [vuInit].concat(tasks);

      // Use async.waterfall to execute all steps serially
      A.waterfall(steps, function done(err, context) {
        if (err) {
          debug("Scenario error:", err);
          ee.emit("error", err);
        }

        // Scenario completed
        return callback(err, context);
      });
    };
  }

  /**
   * Validate engine configuration
   */
  _validateConfig() {
    if (!this.engineConf) {
      throw new Error(
        'Thrift engine configuration is missing. Please add "engines.thrift" section to your Artillery config.'
      );
    }

    if (
      !this.engineConf.services ||
      Object.keys(this.engineConf.services).length === 0
    ) {
      throw new Error(
        'No Thrift services configured. Please add at least one service to "engines.thrift.services".'
      );
    }

    // Validate each service configuration
    Object.keys(this.engineConf.services).forEach((serviceName) => {
      this._validateServiceConfig(
        serviceName,
        this.engineConf.services[serviceName]
      );
    });

    debug("Configuration validation passed");
  }

  /**
   * Validate individual service configuration
   * @param {string} serviceName - Name of the service
   * @param {Object} serviceConfig - Service configuration object
   */
  _validateServiceConfig(serviceName, serviceConfig) {
    const errors = [];

    // Required fields validation
    if (!serviceConfig.module) {
      errors.push(`Service "${serviceName}": "module" is required`);
    }

    if (!serviceConfig.qName) {
      errors.push(
        `Service "${serviceName}": "qName" (qualified name) is required`
      );
    }

    if (!serviceConfig.host) {
      errors.push(`Service "${serviceName}": "host" is required`);
    }

    if (!serviceConfig.port) {
      errors.push(`Service "${serviceName}": "port" is required`);
    } else if (
      typeof serviceConfig.port !== "number" ||
      serviceConfig.port <= 0 ||
      serviceConfig.port > 65535
    ) {
      errors.push(
        `Service "${serviceName}": "port" must be a valid port number (1-65535)`
      );
    }

    // Optional fields validation
    if (
      serviceConfig.transport &&
      !["TBufferedTransport", "TFramedTransport"].includes(
        serviceConfig.transport
      )
    ) {
      errors.push(
        `Service "${serviceName}": "transport" must be "TBufferedTransport" or "TFramedTransport"`
      );
    }

    if (
      serviceConfig.protocol &&
      !["TBinaryProtocol", "TCompactProtocol", "TJSONProtocol"].includes(
        serviceConfig.protocol
      )
    ) {
      errors.push(
        `Service "${serviceName}": "protocol" must be "TBinaryProtocol", "TCompactProtocol", or "TJSONProtocol"`
      );
    }

    // Numeric field validation
    const numericFields = [
      "connectTimeout",
      "timeout",
      "maxAttempts",
      "retryMaxDelay",
    ];
    numericFields.forEach((field) => {
      if (serviceConfig[field] !== undefined) {
        if (
          typeof serviceConfig[field] !== "number" ||
          serviceConfig[field] < 0
        ) {
          errors.push(
            `Service "${serviceName}": "${field}" must be a non-negative number`
          );
        }
      }
    });

    // Specific numeric validations
    if (
      serviceConfig.maxAttempts !== undefined &&
      serviceConfig.maxAttempts < 1
    ) {
      errors.push(`Service "${serviceName}": "maxAttempts" must be at least 1`);
    }

    if (errors.length > 0) {
      throw new Error(`Service configuration errors:\n${errors.join("\n")}`);
    }
  }

  /**
   * Validate Thrift step specification
   * @param {Object} thriftSpec - Thrift step specification
   */
  _validateThriftStep(thriftSpec) {
    const errors = [];

    if (!thriftSpec.service) {
      errors.push('Thrift step: "service" is required');
    } else if (!this.engineConf.services[thriftSpec.service]) {
      errors.push(
        `Thrift step: service "${thriftSpec.service}" is not configured`
      );
    }

    if (!thriftSpec.method) {
      errors.push('Thrift step: "method" is required');
    }

    // Validate capture configuration
    if (thriftSpec.capture) {
      if (
        typeof thriftSpec.capture !== "string" &&
        typeof thriftSpec.capture !== "object"
      ) {
        errors.push('Thrift step: "capture" must be a string or object');
      } else if (typeof thriftSpec.capture === "object") {
        Object.entries(thriftSpec.capture).forEach(([varName, path]) => {
          if (typeof path !== "string") {
            errors.push(
              `Thrift step: capture path for "${varName}" must be a string`
            );
          }
        });
      }
    }

    // Validate match configuration
    if (thriftSpec.match) {
      if (typeof thriftSpec.match !== "object") {
        errors.push('Thrift step: "match" must be an object');
      }
    }

    // Validate continueOnError
    if (
      thriftSpec.continueOnError !== undefined &&
      typeof thriftSpec.continueOnError !== "boolean"
    ) {
      errors.push('Thrift step: "continueOnError" must be a boolean');
    }

    if (errors.length > 0) {
      throw new Error(`Thrift step validation errors:\n${errors.join("\n")}`);
    }
  }

  /**
   * Load Thrift service modules
   */
  _loadServices() {
    Object.keys(this.engineConf.services).forEach((serviceName) => {
      const serviceConfig = this.engineConf.services[serviceName];
      try {
        // Check if module file exists before requiring
        const fs = require("fs");
        const path = require("path");

        let modulePath = serviceConfig.module;
        if (!path.isAbsolute(modulePath)) {
          modulePath = path.resolve(process.cwd(), modulePath);
        }

        if (!fs.existsSync(modulePath)) {
          throw new Error(`Module file does not exist: ${modulePath}`);
        }

        // Dynamically load generated Thrift service code
        this.serviceClasses[serviceName] = require(serviceConfig.module);
        debug(`Loaded service: ${serviceName} from ${serviceConfig.module}`);
      } catch (err) {
        console.error(`Failed to load service ${serviceName}:`, err);
        throw new Error(
          `Cannot load Thrift service module: ${serviceConfig.module}. ${err.message}`
        );
      }
    });
  }

  /**
   * Process individual step
   * @param {Object} rs - Step specification
   * @param {EventEmitter} ee - Event emitter
   */
  _step(rs, ee) {
    const self = this;

    // Handle loops
    if (rs.loop) {
      const steps = rs.loop.map((loopStep) => this._step(loopStep, ee));
      return this.helpers.createLoopWithCount(rs.count || -1, steps, {});
    }

    // Handle logging
    if (rs.log) {
      return function log(context, callback) {
        console.log(self.helpers.template(rs.log, context));
        return process.nextTick(function () {
          callback(null, context);
        });
      };
    }

    // Handle think time (delay)
    if (rs.think) {
      return this.helpers.createThink(
        rs,
        self.engineConf?.defaults?.think || {}
      );
    }

    // Handle custom functions
    if (rs.function) {
      return function (context, callback) {
        let func = self.script.config.processor[rs.function];
        if (!func) {
          return process.nextTick(function () {
            callback(null, context);
          });
        }

        return func(context, ee, function () {
          return callback(null, context);
        });
      };
    }

    // Handle Thrift requests - this is our core functionality
    if (rs.thrift) {
      // Validate Thrift step configuration
      self._validateThriftStep(rs.thrift);

      return function thriftRequest(context, callback) {
        const startedAt = Date.now();

        // Execute Thrift call
        self
          ._executeThriftCall(rs.thrift, context, ee)
          .then((result) => {
            const endedAt = Date.now();
            const latency = endedAt - startedAt;

            // Send response event
            const requestName = `${rs.thrift.service}.${rs.thrift.method}`;
            ee.emit("response", latency, 0, requestName);

            // Emit histogram metric for response time
            ee.emit("histogram", "thrift.response_time", latency);

            // Handle response capture
            if (rs.thrift.capture) {
              self.captureResponse(result, rs.thrift.capture, context);
            }

            // Handle response validation
            if (rs.thrift.match) {
              self.validateResponse(result, rs.thrift.match, context, ee);
            }

            debug(`Thrift call ${requestName} completed in ${latency}ms`);
            callback(null, context);
          })
          .catch((err) => {
            const endedAt = Date.now();
            const latency = endedAt - startedAt;

            const requestName = `${rs.thrift.service}.${rs.thrift.method}`;
            ee.emit("response", latency, err.code || 1, requestName);
            ee.emit("error", err);

            // Emit histogram metric for response time
            ee.emit("histogram", "thrift.response_time", latency);

            debug(`Thrift call ${requestName} failed:`, err);

            // If continueOnError is set, continue execution
            if (rs.thrift.continueOnError) {
              return callback(null, context);
            }

            callback(err, context);
          });

        // Send request event
        ee.emit("request");

        // Emit counter and rate metrics for thrift requests
        ee.emit("counter", "thrift.requests", 1);
        ee.emit("rate", "thrift.request_rate");
      };
    }

    // Ignore unrecognized operations
    return function doNothing(context, callback) {
      debug(`unrecognized step: ${JSON.stringify(rs)}`);
      return callback(null, context);
    };
  }

  /**
   * Execute Thrift call
   */
  async _executeThriftCall(spec, context, ee) {
    // Get or create client
    const client = await this._getOrCreateClient(spec.service, context);

    // Prepare arguments - supports template substitution
    const args = this._prepareArguments(spec.args, context);

    // Get method
    const method = client[spec.method];
    if (!method) {
      throw new Error(
        `Method ${spec.method} not found on service ${spec.service}`
      );
    }

    // Execute call
    return new Promise((resolve, reject) => {
      const callback = (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      };

      // Call method
      method.apply(client, [...args, callback]);
    });
  }

  /**
   * Get or create Thrift client
   */
  async _getOrCreateClient(serviceName, context) {
    const serviceConfig = this.engineConf.services[serviceName];
    if (!serviceConfig) {
      throw new Error(`Service ${serviceName} not configured`);
    }

    const key = `${serviceConfig.host}:${serviceConfig.port}:${serviceName}`;

    // Check cache
    if (this.clients.has(key)) {
      return this.clients.get(key);
    }

    // Create new connection
    const transport = this.getTransport(serviceConfig.transport);
    const protocol = this.getProtocol(serviceConfig.protocol);

    const connection = thrift.createConnection(
      serviceConfig.host,
      serviceConfig.port,
      {
        transport: transport,
        protocol: protocol,
        max_attempts: serviceConfig.maxAttempts || 3,
        retry_max_delay: serviceConfig.retryMaxDelay || 1000,
        connect_timeout: serviceConfig.connectTimeout || 5000,
        timeout: serviceConfig.timeout || 10000,
      }
    );

    // Error handling
    connection.on("error", (err) => {
      console.error(`Connection error for ${key}:`, err);
      this.clients.delete(key);
      this.connections.delete(key);
    });
    
    // Create client
    const Service = this.serviceClasses[serviceName];
    // const client = thrift.createClient(Service, connection);

    const multiplexer = new thrift.Multiplexer();
    const client = multiplexer.createClient(
      this.engineConf.services[serviceName].qName, // Fully qualified name of BusinessService
      Service, // Thrift interface
      connection // Basic TCP connection created above
    );

    // Cache
    this.connections.set(key, connection);
    this.clients.set(key, client);

    debug(`Created new client for ${key}`);

    return client;
  }

  /**
   * Get transport layer
   */
  getTransport(transportName) {
    const transports = {
      TBufferedTransport: thrift.TBufferedTransport,
      TFramedTransport: thrift.TFramedTransport,
    };
    return transports[transportName] || thrift.TBufferedTransport;
  }

  /**
   * Get protocol
   */
  getProtocol(protocolName) {
    const protocols = {
      TBinaryProtocol: thrift.TBinaryProtocol,
      TCompactProtocol: thrift.TCompactProtocol,
      TJSONProtocol: thrift.TJSONProtocol,
    };
    return protocols[protocolName] || thrift.TBinaryProtocol;
  }

  /**
   * Prepare arguments - handle template variables
   */
  _prepareArguments(args, context) {
    if (!args) return [];

    if (Array.isArray(args)) {
      return args.map((arg) => this.processArgument(arg, context));
    }

    return [this.processArgument(args, context)];
  }

  /**
   * Process individual argument
   */
  processArgument(arg, context) {
    // String template substitution
    if (typeof arg === "string") {
      return this.helpers.template(arg, context, true);
    }

    // Recursive object processing
    if (typeof arg === "object" && arg !== null) {
      if (Array.isArray(arg)) {
        return arg.map((item) => this.processArgument(item, context));
      }

      const processed = {};
      for (const [key, value] of Object.entries(arg)) {
        processed[key] = this.processArgument(value, context);
      }
      return processed;
    }

    return arg;
  }

  /**
   * Capture response data
   */
  captureResponse(response, capture, context) {
    context.vars = context.vars || {};

    if (typeof capture === "string") {
      // Simple capture
      context.vars[capture] = response;
    } else if (typeof capture === "object") {
      // Path capture
      for (const [varName, path] of Object.entries(capture)) {
        const value = this.extractValue(response, path);
        context.vars[varName] = value;
        debug(`Captured ${varName} = ${JSON.stringify(value)}`);
      }
    }
  }

  /**
   * Validate response
   */
  validateResponse(response, matchers, context, ee) {
    for (const [path, expected] of Object.entries(matchers)) {
      const actual = this.extractValue(response, path);
      const expectedValue = this.helpers.template(expected, context, true);

      const success = JSON.stringify(actual) === JSON.stringify(expectedValue);

      ee.emit("match", success, {
        path,
        expected: expectedValue,
        actual,
        success,
      });

      if (!success) {
        debug(
          `Validation failed: ${path} expected ${expectedValue}, got ${actual}`
        );
      }
    }
  }

  /**
   * Extract value from response
   */
  extractValue(obj, path) {
    if (path === "$") return obj;

    const keys = path.replace(/^\$\./, "").split(".");
    let value = obj;

    for (const key of keys) {
      if (value && typeof value === "object") {
        value = value[key];
      } else {
        return undefined;
      }
    }

    return value;
  }

  /**
   * Clean up resources
   */
  cleanup() {
    debug("Cleaning up connections...");

    for (const [key, connection] of this.connections) {
      try {
        connection.end();
        debug(`Closed connection: ${key}`);
      } catch (err) {
        debug(`Error closing connection ${key}:`, err);
      }
    }

    this.connections.clear();
    this.clients.clear();
  }
}

module.exports = ThriftEngine;
