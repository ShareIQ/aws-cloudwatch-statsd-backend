'use strict'

const AWS = require('aws-sdk')

class CloudwatchBackend {
  constructor (startupTime, config, emitter) {
    this.config = config || {}
    AWS.config = this.config
    this.metricDataByNamespace = {}

    const setEmitter = () => {
      this.cloudwatch = new AWS.CloudWatch(this.config)
      emitter.on('flush', (timestamp, metrics) => {
        this.flush(timestamp, metrics)
      })
    }

    // if iamRole is set attempt to fetch credentials from the Metadata Service
    if (this.config.iamRole) {
      if (this.config.iamRole === 'any') {
        // If the iamRole is set to any, then attempt to fetch any available credentials
        const ms = new AWS.EC2MetadataCredentials()
        ms.refresh((err) => {
          if (err) {
            console.log('Failed to fetch IAM role credentials:', err)
          }
          this.config.credentials = ms
          setEmitter()
        })
      } else {
        // however if it's set to specify a role, query it specifically.
        const ms = new AWS.MetadataService()
        ms.request(
          `/latest/meta-data/iam/security-credentials/${this.config.iamRole}`,
          (err, rdata) => {
            const data = JSON.parse(rdata)
            if (err) {
              console.log('Failed to fetch IAM role credentials:', err)
            }
            this.config.credentials = new AWS.Credentials(
              data.AccessKeyId, data.SecretAccessKey, data.Token
            )
            setEmitter()
          }
        )
      }
    } else {
      setEmitter()
    }
  }

  processKey (key) {
    const parts = key.split(/[\.\/-]/)
    return {
      metricName: parts[parts.length - 1],
      namespace: parts.length > 1
        ? parts.splice(0, parts.length - 1).join('/')
        : null
    }
  }

  registerMetric (key, metricData) {
    const names = this.config.processKeyForNamespace ? this.processKey(key) : {}
    const namespace = this.config.namespace || names.namespace || 'AwsCloudWatchStatsdBackend'
    const metricName = this.config.metricName || names.metricName || key
    if (!this.metricDataByNamespace[namespace]) {
      this.metricDataByNamespace[namespace] = []
    }
    metricData.MetricName = metricName
    this.metricDataByNamespace[namespace].push(metricData)
  }

  prepareMetrics (timestamp, metrics, unit, fn) {
    for (let key in metrics) {
      /*
      if (key.indexOf('statsd.') == 0) {
        continue
      }
      */

      if (this.config.whitelist && this.config.whitelist.length > 0 && this.config.whitelist.indexOf(key) === -1) {
        console.log(`Key "${key}" not in whitelist`)
        continue
      }

      this.registerMetric(key, {
        Unit: unit,
        Timestamp: new Date(timestamp * 1000).toISOString(),
        Value: fn ? fn(metrics[key], key) : metrics[key]
      })
    }
  }

  prepareStatMetrics (timestamp, metrics) {
    for (let key in metrics) {
      if (metrics[key].length > 0) {
        if (this.config.whitelist && this.config.whitelist.length > 0 && this.config.whitelist.indexOf(key) === -1) {
          console.log(`Key "${key}" not in whitelist`)
          continue
        }

        this.registerMetric(key, {
          Unit: 'Milliseconds',
          Timestamp: new Date(timestamp * 1000).toISOString(),
          StatisticValues: {
            Minimum: Math.min.apply(null, metrics[key]),
            Maximum: Math.max.apply(null, metrics[key]),
            Sum: metrics[key].reduce((a, b) => a + b),
            SampleCount: metrics[key].length
          }
        })
      }
    }
  }

  flush (timestamp, metrics) {
    console.log(`Flushing metrics at ${new Date(timestamp * 1000).toISOString()}`)
    this.prepareMetrics(timestamp, metrics.counters, 'Count')
    this.prepareMetrics(timestamp, metrics.gauges, 'None')
    this.prepareMetrics(timestamp, metrics.sets, 'None', (v) => v.values().length)
    this.prepareStatMetrics(timestamp, metrics.timers)

    Object.keys(this.metricDataByNamespace).forEach((namespace) => {
      console.log(`Flushing ${namespace}`, this.metricDataByNamespace[namespace])
      this.cloudwatch.putMetricData(
        {
          MetricData: this.metricDataByNamespace[namespace],
          Namespace: namespace
        },
        (err, data) => {
          console.error(err)
          console.log(data)
          delete this.metricDataByNamespace[namespace]
        }
      )
    })
  }
}

exports.init = (startupTime, config, events) => {
  console.log('Config:', config)
  const cloudwatch = config.cloudwatch || {}
  const instances = cloudwatch.instances || [cloudwatch]
  for (const key in instances) {
    const instanceConfig = instances[key]
    console.log(`Starting cloudwatch reporter instance in region ${instanceConfig.region}`)
    const instance = new CloudwatchBackend(startupTime, instanceConfig, events)
  }
  return true
}
