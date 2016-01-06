export no_proxy="griffin-objstore.opensciencedatacloud.org"
function with_proxy() {
    PROXY="http://cloud-proxy:3128"
    http_proxy="${PROXY}" https_proxy="${PROXY}" ftp_proxy="${PROXY}" $@
}
with_proxy wget -m ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year
