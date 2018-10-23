---
title: Enrichers
layout: single
toc: true
---

The Base Enrich is a simple enricher that complements the information of received messages.

The enrichment system can be either SQL or NoSQL database, a CSV file or any other data store system.

![](../assets/images/basenrich_concept.png?raw=true)

Image above represents the base enrich behaviour:

1. Enricher receives a message from `stream` topic.
2. Enricher enrich the received message with a external data store system.
3. Enricher sends the enrich message to `output` topic.

***It important to emphasise that a message doesn't need to have a key like the joiners.***

### GeoIpEnrich
The GeoIpEnrich allow us enrich streams with information about IP location, internally the enricher uses database in order to determine the IP location.

```json
{
  "name":"geoIp",
  "className":"io.wizzie.enricher.enrichment.geoip.GeoIpEnrichr",
  "properties": {
    "src.dim": "src_ip",
    "dst.dim": "dst_ip",
    "src.country.code.dim": "src_country_code",
    "dst.country.code.dim": "dst_country_code",
    "src.as.name.dim": "src_as_name",
    "dst.as.name.dim": "dst_as_name",
    "asn6.db.path": "/opt/enricher/data/asnv6.dat",
    "asn.db.path": "/opt/enricher/data/asn.dat",
    "city6.db.path": "/opt/enricher/data/cityv6.dat",
    "city.db.path": "/opt/enricher/data/city.dat"
  }
}
```

On this enricher you have next properties:

* `src.dim`: The dimension that contains source IP.
* `dst.dim`: The dimension that contains destiny IP.
* `src.country.code.dim`: The target dimension for country code information about source IP.
* `dst.country.code.dim`: The target dimension for country code information about destiny IP.
* `src.as.name.dim`: The target dimension for name information about source IP.
* `dst.as.name.dim`: The target dimension for name information about destiny IP.
* `asn6.db.path`: Path to IPv6 ASN database.
* `asn.db.path`: Path to IPv4 ASN database.
* `city6.db.path`: Path to IPv6 city database.
* `city.db.path`: Path to IPv4 city database.

This enricher process a message and return both source and destiny IP location. Not all IP are localizable.

**Input:**

```json
{"src": "8.8.8.8", "dst": "8.8.8.8"}
```

***Output:***

```json
{"src": "8.8.8.8", "dst": "8.8.8.8", "dst_country_code":"US", "src_as_name":"Google Inc.", "dst_as_name":"Google Inc.", "src_country_code":"US"}
```

### MacVendorEnrich
The MacVendorEnrich allow us enrich streams with information about Mac vendors internally the enricher uses database in order to determine the mac vendor.

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

On this enricher you have next properties:

* `oui.file.path`: Path to mac vendors database file.
* `mac.dim`: The dimension that contains mac address.
* `mac.vendor.dim`: The target dimension for vendor's name about mac address.

This enricher process a message and return the mac vendor field. Not all mac address have mac vendor.

**Input:**
```json
{"client_mac": "3c:ab:8e:12:34:56"}
```

***Output:**

```json
{"client_mac": "3c:ab:8e:12:34:56", "client_mac_vendor":"Apple"}
```
