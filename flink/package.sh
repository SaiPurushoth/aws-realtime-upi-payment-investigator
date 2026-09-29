#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
build_dir="${script_dir}/build"
staging_dir="${build_dir}/staging"

mvn -f "${script_dir}/pom.xml" clean package
rm -rf "${staging_dir}"
mkdir -p "${staging_dir}/lib"
cp "${script_dir}/main.py" "${staging_dir}/main.py"
cp "${script_dir}/investigator.py" "${staging_dir}/investigator.py"
cp "${script_dir}/target/pyflink-dependencies.jar" \
  "${staging_dir}/lib/pyflink-dependencies.jar"

(
  cd "${staging_dir}"
  zip -q -r "${build_dir}/upi-flink-app.zip" main.py investigator.py lib
)
rm -rf "${staging_dir}"
echo "Created ${build_dir}/upi-flink-app.zip"
