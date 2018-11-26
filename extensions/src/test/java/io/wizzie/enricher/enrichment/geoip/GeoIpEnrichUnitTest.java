package io.wizzie.enricher.enrichment.geoip;


import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.wizzie.enricher.enrichment.utils.Constants.ASN_DB_PATH;
import static io.wizzie.enricher.enrichment.utils.Constants.CITY_DB_PATH;
import static org.junit.Assert.assertEquals;

public class GeoIpEnrichUnitTest {

    @Test
    public void enrichWithGeoIp() {
        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File asn = new File(classLoader.getResource("GeoLite2-ASN.mmdb").getFile());
        File city = new File(classLoader.getResource("GeoLite2-City.mmdb").getFile());

        Map<String, Object> properties = new HashMap<>();
        properties.put(ASN_DB_PATH, asn.getAbsolutePath());
        properties.put(CITY_DB_PATH, city.getAbsolutePath());
        properties.put("src.country.code.dim", "src_country_code");
        properties.put("dst.country.code.dim", "dst_country_code");
        properties.put("src.dim", "src");
        properties.put("dst.dim", "dst");
        properties.put("src.as.name.dim", "src_as_name");
        properties.put("dst.as.name.dim", "dst_as_name");
        geoIpEnrich.init(properties, null);

        Map<String, Object> message = new HashMap<>();
        message.put("src", "8.8.8.8");
        message.put("dst", "8.8.4.4");

        Map<String, Object> expected = new HashMap<>(message);
        expected.put("dst_country_code", "US");
        expected.put("src_country_code", "US");
        expected.put("dst_as_name", "Google LLC");
        expected.put("src_as_name", "Google LLC");
        expected.put("src_longitude", -97.822);
        expected.put("src_latitude", 37.751);
        expected.put("dst_longitude", -97.822);
        expected.put("dst_latitude", 37.751);

        Map<String, Object> result = geoIpEnrich.enrich(message);

        assertEquals(expected, result);

        geoIpEnrich.stop();
    }

    @Test
    public void enrichWithGeoIpV6() {
        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File asn = new File(classLoader.getResource("GeoLite2-ASN.mmdb").getFile());
        File city = new File(classLoader.getResource("GeoLite2-City.mmdb").getFile());

        Map<String, Object> properties = new HashMap<>();
        properties.put(ASN_DB_PATH, asn.getAbsolutePath());
        properties.put(CITY_DB_PATH, city.getAbsolutePath());
        properties.put("src.country.code.dim", "src_country_code");
        properties.put("dst.country.code.dim", "dst_country_code");
        properties.put("src.dim", "src");
        properties.put("dst.dim", "dst");
        properties.put("src.as.name.dim", "src_as_name");
        properties.put("dst.as.name.dim", "dst_as_name");
        geoIpEnrich.init(properties, null);

        Map<String, Object> message = new HashMap<>();
        message.put("src", "2001:4860:4860::8888");
        message.put("dst", "2001:4860:4860::8844");

        Map<String, Object> expected = new HashMap<>(message);
        expected.put("dst_country_code", "US");
        expected.put("src_country_code", "US");
        expected.put("dst_as_name", "Google LLC");
        expected.put("src_as_name", "Google LLC");
        expected.put("src_longitude", -97.822);
        expected.put("src_latitude", 37.751);
        expected.put("dst_longitude", -97.822);
        expected.put("dst_latitude", 37.751);

        Map<String, Object> result = geoIpEnrich.enrich(message);

        assertEquals(expected, result);

        geoIpEnrich.stop();
    }

    @Test
    public void enrichWithGeoIpInVerboseMode() {
        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File asn = new File(classLoader.getResource("GeoLite2-ASN.mmdb").getFile());
        File city = new File(classLoader.getResource("GeoLite2-City.mmdb").getFile());

        Map<String, Object> properties = new HashMap<>();
        properties.put(ASN_DB_PATH, asn.getAbsolutePath());
        properties.put(CITY_DB_PATH, city.getAbsolutePath());
        properties.put("src.dim", "src");
        properties.put("dst.dim", "dst");
        properties.put("enable.data.verbose.mode", true);
        geoIpEnrich.init(properties, null);

        Map<String, Object> message = new HashMap<>();
        message.put("src", "8.8.8.8");
        message.put("dst", "8.8.4.4");

        Map<String, Object> expected = new HashMap<>(message);
        expected.put("dst_country_code", "US");
        expected.put("src_country_code", "US");
        expected.put("dst_country_name", "United States");
        expected.put("src_country_name", "United States");
        expected.put("dst_country_iso_code", "US");
        expected.put("src_country_iso_code", "US");
        expected.put("dst_as_name", "Google LLC");
        expected.put("src_as_name", "Google LLC");
        expected.put("src_longitude", -97.822);
        expected.put("src_latitude", 37.751);
        expected.put("dst_longitude", -97.822);
        expected.put("dst_latitude", 37.751);
        expected.put("src_continent_name", "North America");
        expected.put("dst_continent_name", "North America");
        expected.put("src_continent_code", "NA");
        expected.put("dst_continent_code", "NA");
        expected.put("src_is_anonymous", false);
        expected.put("dst_is_anonymous", false);
        expected.put("src_is_public_proxy", false);
        expected.put("dst_is_public_proxy", false);
        expected.put("src_is_legitimate_proxy", false);
        expected.put("dst_is_legitimate_proxy", false);
        expected.put("src_is_hosting_provider", false);
        expected.put("dst_is_hosting_provider", false);
        expected.put("src_is_tor_exit_node", false);
        expected.put("dst_is_tor_exit_node", false);
        expected.put("src_is_anonymous_vpn", false);
        expected.put("dst_is_anonymous_vpn", false);

        Map<String, Object> result = geoIpEnrich.enrich(message);

        assertEquals(expected, result);

        geoIpEnrich.stop();
    }

    @Test
    public void enrichWithGeoIpV6InVerboseMode() {
        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File asn = new File(classLoader.getResource("GeoLite2-ASN.mmdb").getFile());
        File city = new File(classLoader.getResource("GeoLite2-City.mmdb").getFile());

        Map<String, Object> properties = new HashMap<>();
        properties.put(ASN_DB_PATH, asn.getAbsolutePath());
        properties.put(CITY_DB_PATH, city.getAbsolutePath());
        properties.put("src.dim", "src");
        properties.put("dst.dim", "dst");
        properties.put("enable.data.verbose.mode", true);
        geoIpEnrich.init(properties, null);

        Map<String, Object> message = new HashMap<>();
        message.put("src", "2001:4860:4860::8888");
        message.put("dst", "2001:4860:4860::8844");

        Map<String, Object> expected = new HashMap<>(message);
        expected.put("dst_country_code", "US");
        expected.put("src_country_code", "US");
        expected.put("dst_country_name", "United States");
        expected.put("src_country_name", "United States");
        expected.put("dst_country_iso_code", "US");
        expected.put("src_country_iso_code", "US");
        expected.put("dst_as_name", "Google LLC");
        expected.put("src_as_name", "Google LLC");
        expected.put("src_longitude", -97.822);
        expected.put("src_latitude", 37.751);
        expected.put("dst_longitude", -97.822);
        expected.put("dst_latitude", 37.751);
        expected.put("src_continent_name", "North America");
        expected.put("dst_continent_name", "North America");
        expected.put("src_continent_code", "NA");
        expected.put("dst_continent_code", "NA");
        expected.put("src_is_anonymous", false);
        expected.put("dst_is_anonymous", false);
        expected.put("src_is_public_proxy", false);
        expected.put("dst_is_public_proxy", false);
        expected.put("src_is_legitimate_proxy", false);
        expected.put("dst_is_legitimate_proxy", false);
        expected.put("src_is_hosting_provider", false);
        expected.put("dst_is_hosting_provider", false);
        expected.put("src_is_tor_exit_node", false);
        expected.put("dst_is_tor_exit_node", false);
        expected.put("src_is_anonymous_vpn", false);
        expected.put("dst_is_anonymous_vpn", false);


        Map<String, Object> result = geoIpEnrich.enrich(message);

        assertEquals(expected, result);

        geoIpEnrich.stop();
    }

    @Test
    public void defaultPropertiesShouldWorkCorrectly() {
        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();
        Map<String, Object> properties = new HashMap<>();

        geoIpEnrich.init(properties, null);

        String[] baseDimensions = {
                "country_code",
                "country_is_in_european_union",
                "country_name",
                "country_iso_code",
                "continent_name",
                "continent_code",
                "city",
                "longitude",
                "latitude",
                "timezone",
                "as_name",
                "is_anonymous",
                "is_anonymous_vpn",
                "is_hosting_provider",
                "is_tor_exit_node",
                "is_public_proxy",
                "is_legitimate_proxy",
                "timezone",
                "postal_code"
        };

        for (String dimension : baseDimensions) {
            assertEquals(String.format("src_%s", dimension), geoIpEnrich.srcDimensionsMap.get(dimension));
            assertEquals(String.format("dst_%s", dimension), geoIpEnrich.dstDimensionsMap.get(dimension));
        }
    }

    @Test
    public void dimensionNameShouldBeCorrectly(){
        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();
        Map<String, Object> properties = new HashMap<>();

        String[] baseDimensions = {
                "country_code",
                "country_is_in_european_union",
                "country_name",
                "country_iso_code",
                "continent_name",
                "continent_code",
                "city",
                "longitude",
                "latitude",
                "timezone",
                "as_name",
                "is_anonymous",
                "is_anonymous_vpn",
                "is_hosting_provider",
                "is_tor_exit_node",
                "is_public_proxy",
                "is_legitimate_proxy",
                "timezone",
                "postal_code"
        };

        Map<String, String> srcExpectedValues = new HashMap<>();
        String srcDimUUID = UUID.randomUUID().toString();
        properties.put("src.dim", srcDimUUID);

        Map<String, String> dstExpectedValues = new HashMap<>();
        String dstDimUUID = UUID.randomUUID().toString();
        properties.put("dst.dim", dstDimUUID);

        for (String base : baseDimensions) {
            String property = String.format("src.%s.dim", base.replace("_", "."));
            String uuid = UUID.randomUUID().toString();
            properties.put(property, uuid);
            srcExpectedValues.put(base, uuid);
        }

        for (String base : baseDimensions) {
            String property = String.format("dst.%s.dim", base.replace("_", "."));
            String uuid = UUID.randomUUID().toString();
            properties.put(property, uuid);
            dstExpectedValues.put(base, uuid);
        }

        geoIpEnrich.init(properties, null);

        assertEquals(srcDimUUID, geoIpEnrich.SRC_IP);
        for (String dimension : baseDimensions) {
            assertEquals(srcExpectedValues.get(dimension), geoIpEnrich.srcDimensionsMap.get(dimension));
        }

        assertEquals(dstDimUUID, geoIpEnrich.DST_IP);
        for (String dimension : baseDimensions) {
            assertEquals(dstExpectedValues.get(dimension), geoIpEnrich.dstDimensionsMap.get(dimension));
        }
    }
}
