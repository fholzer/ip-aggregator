package com.gvcgroup.ipaggregator.model;

import java.util.Objects;

/**
 *
 * @author Ferdinand Holzer
 */
public class Isp {
    private final Integer asn;
    private final String aso;
    private final String isp;
    private final String org;

    public Isp(Integer asn, String aso, String isp, String org) {
        this.asn = asn;
        this.aso = aso;
        this.isp = isp;
        this.org = org;
    }

    public Integer getAsn() {
        return asn;
    }

    public String getAso() {
        return aso;
    }

    public String getIsp() {
        return isp;
    }

    public String getOrg() {
        return org;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 73 * hash + Objects.hashCode(this.asn);
        hash = 73 * hash + Objects.hashCode(this.aso);
        hash = 73 * hash + Objects.hashCode(this.isp);
        hash = 73 * hash + Objects.hashCode(this.org);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Isp other = (Isp) obj;
        if (!Objects.equals(this.aso, other.aso)) {
            return false;
        }
        if (!Objects.equals(this.isp, other.isp)) {
            return false;
        }
        if (!Objects.equals(this.org, other.org)) {
            return false;
        }
        if (!Objects.equals(this.asn, other.asn)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Isp{" + "asn=" + asn + ", aso=" + aso + ", isp=" + isp + ", org=" + org + '}';
    }


}
