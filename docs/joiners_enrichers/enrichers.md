---
title: Enrichers
layout: single
toc: true
---

The Base Enrich is a simple enricher that complements the information of received messages.

The enrichment system can be either SQL or NoSQL database, a CSV file or any other data store system.

<figure>
    <a href="{{ '/assets/images/simple_enrich.png' | relative_url }}"><img src="{{ '/assets/images/simple_enrich.png' | relative_url }}"></a>
</figure>

Image above represents the base enrich behaviour:

1. Enricher receives a message from `stream` topic.
2. Enricher enrich the received message with a external data store system.
3. Enricher sends the enrich message to `output` topic.

**It important to emphasise that a message doesn't need to have a key like the joiners.**
{: .notice--info}

### StaticDataEnrich
The StaticDataEnrich allows us to enrich streams with the static information given by us. You can define the data in the property section:

```json
{
  "name": "staticDataEnrich",
  "className": "io.wizzie.enricher.enrichment.simple.impl.StaticDataEnrich",
  "properties": {
    "DIMENSION-A": [
        {
           "ifValueIsEqualTo": "VALUE-A",
           "enrichWith": {"field1": "value1"}
        },
        {
            "ifValueIsEqualTo": "VALUE-B",
            "enrichWith": {"field2": true, "field3": 17}
        }
    ],
    "DIMENSION-B": [
        {
            "ifValueIsEqualTo": "VALUE-C",
            "enrichWith": {"field4": 1.23}
        }
    ]
  }
}
```

On this enricher you have the next properties:

* `ifValueIsEqualTo`: Check if value of specified dimension is equal to provide value.
* `enrichWith`: The dimension that contains the json map to enrich.

This enricher processes a message and returns the defined static fields.

**Input:**
```json
{"DIMENSION-A": "VALUE-A"}
{"DIMENSION-A": "VALUE-B"}
{"DIMENSION-B": "VALUE-C"}
```

**Output:**

```json
{"DIMENSION-A": "VALUE-B", "field1": "value1"}
{"DIMENSION-A": "VALUE-B", "field2": true, "field3": 17}
{"DIMENSION-A": "VALUE-B", "field4": 1.23}

```

### GeoIpEnrich
The GeoIpEnrich allows us enrich streams with information about IP location, internally the enricher uses MaxMind [databases](https://dev.maxmind.com/geoip/geoip2/geolite2/) in order to determine the IP information.

```json
{
  "name":"geoIp",
  "className":"io.wizzie.enricher.enrichment.geoip.GeoIpEnricher",
  "properties": {
    "src.dim": "src_ip",
    "dst.dim": "dst_ip",
    "src.country.code.dim": "src_country_code",
    "dst.country.code.dim": "dst_country_code",
    "src.as.name.dim": "src_as_name",
    "dst.as.name.dim": "dst_as_name",
    "asn.db.path": "/opt/enricher/data/GeoLite2-ASN.mmdb",
    "city.db.path": "/opt/enricher/data/GeoLite2-City.mmdb"
  }
}
```

On this enricher you have the next properties:

**Dimensions**: You can set the dimensions' name with the next properties.

| Property    | Description     |  Default Value|
|:-------------:|:-----------------------------:|:-------------:|
| `src.dim`             | The dimension that contains source IP |  `src` |
| `dst.dim`             | The dimension that contains destiny IP | `dst` |
|`src.as.name.dim` | The target dimension for name information about source IP| `src_as_name`|
|`dst.as.name.dim`| The target dimension for name information about destiny IP| `dst_as_name`|
| `src.country.code.dim`| The target dimension for country code information about source IP | `src_country_code`|
| `dst.country.code.dim`| The target dimension for country code information about destiny IP | `dst_country_code`|
| `src.country.iso.code.dim`| The target dimension for country iso code information about source IP | `src_country_iso_code`|
| `dst.country.iso.code.dim`| The target dimension for country iso code information about destiny IP | `dst_country_iso_code`|
| `src.country.is .in.european.union.dim`| The target dimension for country is in european union information about source IP | `src_country_is_in_european_union`|
| `dst.country.is .in.european.union.dim`| The target dimension for country is in european union information about destiny IP | `dst_country_is_in_european_union`|
|`src.country.name.dim`| The target dimension for country name information about source IP| `src_country_name`|
|`dst.country.name.dim`| The target dimension for country name information about destiny IP| `dst_country_name`|
|`src.continent.name.dim`| The target dimension for continent name information about source IP| `src_continent_name`|
|`dst.continent.name.dim`| The target dimension for continent name information about destiny IP| `dst_continent_name`|
|`src.continent.code.dim`| The target dimension for continent code information about source IP| `src_continent_code`|
|`dst.continent.code.dim`| The target dimension for continent code information about destiny IP| `dst_continent_code`|
|`src.city.name.dim`| The target dimension for city information about source IP| `src_city_name`|
|`dst.city.name.dim`| The target dimension for city information about destiny IP| `dst_city_name`|
|`src.postal.code.dim`| The target dimension for postal code information about source IP| `src_postal_code`|
|`dst.postal.code.dim`| The target dimension for postal code information about destiny IP| `dst_postal_code`|
|`src.latitude.dim`| The target dimension for latitude information about source IP| `src_latitude`|
|`dst.latitude.dim`| The target dimension for latitude information about destiny IP| `dst_latitude`|
|`src.longitude.dim`| The target dimension for longitude information about source IP| `src_longitude`|
|`dst.longitude.dim`| The target dimension for longitude information about destiny IP| `dst_longitude`|
|`src.is.anonymous.dim`| The target dimension for is anonymous information about source IP| `src_is_anonymous`|
|`dst.is.anonymous.dim`| The target dimension for is anonymous information about destiny IP| `dst_is_anonymous`|
|`src.is.anonymous.vpn.dim`| The target dimension for is anonymous vpn information about source IP| `src_is_anonymous_vpn`|
|`dst.is.anonymous.vpn.dim`| The target dimension for is anonymous vpn information about destiny IP| `dst_is_anonymous_vpn`|
|`src.is.hosting.provider.dim`| The target dimension for is hosting provider information about source IP| `src_is_hosting_provider`|
|`dst.is.hosting.provider.dim`| The target dimension for is hosting provider information about destiny IP| `dst_is_hosting_provider`|
|`src.is.tor.exit.node.dim`| The target dimension for is tor exit node information about source IP| `src_is_tor_exit_node`|
|`dst.is.tor.exit.node.dim`| The target dimension for is tor exit node information about destiny IP| `dst_is_tor_exit_node`|
|`src.is.public.proxy.dim`| The target dimension for is public proxy information about source IP| `src_is_public_proxy`|
|`dst.is.public.proxy.dim`| The target dimension for is public proxy information about destiny IP| `dst_is_public_proxy`|
|`src.is.legitimate.proxy.dim`| The target dimension for is legitimate proxy information about source IP| `src_is_legitimate_proxy`|
|`dst.is.legitimate.proxy.dim`| The target dimension for is legitimate proxy information about destiny IP| `dst_is_legitimate_proxy`|
|`src.timezone.dim`| The target dimension for timezone information about source IP| `src_timezone`|
|`dst.timezone.dim`| The target dimension for timezone information about destiny IP| `dst_timezone`|


**Databases**: You can define the path of databases using the next properties:

| Property    | Description     |  Default Value|
|:-------------:|:-----------------------------:|:-------------:|
|`asn.db.path`|Path to ASN database|/opt/share/GeoIP2/GeoLite2-ASN.mmdb|
|`city.db.path`|Path to city database|/opt/share/GeoIP2/GeoLite2-City.mmdb|

**Other properties**: You can use the next properties:

| Property    | Description     |  Default Value|
|:-------------:|:-----------------------------:|:-------------:|
|`enable.data.verbose.mode`|Allow you get additional information if it's enable|false|



**Input:**

```json
{"src": "8.8.8.8", "dst": "8.8.4.4"}
```

***Output:***

```json
{
    "src": "8.8.8.8",
    "dst": "8.8.8.8",
    "dst_country_code": "US",
    "src_country_code": "US",
    "src_as_name": "Google LLC",
    "dst_as_name": "Google LLC",
    "src_longitude": -97.822,
    "src_latitude": 37.751,
    "dst_longitude": -97.822,
    "dst_latitude": 37.751
}
```

With `enable.data.verbose.mode` property to `true` you will get:

**Input:**

```json
{"src": "2001:4860:4860::8888", "dst": "2001:4860:4860::8844"}
```

***Output:***

```json
{
    "src": "2001:4860:4860::8888",
    "dst": "2001:4860:4860::8844",
    "dst_country_code": "US",
    "src_country_code": "US",
    "src_as_name": "Google LLC",
    "dst_as_name": "Google LLC",
    "src_longitude": -97.822,
    "src_latitude": 37.751,
    "dst_longitude": -97.822,
    "dst_latitude": 37.751,
    "src_country_name": "United States",
    "dst_country_name": "United States",
    "src_continent_name": "North America",
    "dst_continent_name": "North America",
    "src_continent_code": "NA",
    "dst_continent_code": "NA",
    "src_is_anonymous": false,
    "dst_is_anonymous": false,
    "src_is_public_proxy": false,
    "dst_is_public_proxy": false,
    "src_is_legitimate_proxy": false,
    "dst_is_legitimate_proxy": false,
    "src_is_hosting_provider": false,
    "dst_is_hosting_provider": false,
    "src_is_tor_exit_node": false,
    "dst_is_tor_exit_node": false,
    "src_is_anonymous_vpn": false,
    "dst_is_anonymous_vpn": false
}
```

### MacVendorEnrich
The MacVendorEnrich allows us enrich streams with information about Mac vendors internally the enricher uses database in order to determine the mac vendor.

```json
{
  "name": "macVendor",
  "className": "io.wizzie.enricher.enrichment.MacVendorEnrich",
  "properties": {
    "oui.file.path": "/opt/enricher/data/mac_vendors",
    "mac.dim": "client_mac",
    "mac.vendor.dim": "client_mac_vendor"
  }
}
```

On this enricher you have the next properties:

* `oui.file.path`: Path to mac vendors database file.
* `mac.dim`: The dimension that contains mac address.
* `mac.vendor.dim`: The target dimension for vendor's name about mac address.

This enricher processes a message and returns the mac vendor field. Not all mac address have mac vendor.

**Input:**
```json
{"client_mac": "3c:ab:8e:12:34:56"}
```

**Output:**

```json
{"client_mac": "3c:ab:8e:12:34:56", "client_mac_vendor":"Apple"}
```
