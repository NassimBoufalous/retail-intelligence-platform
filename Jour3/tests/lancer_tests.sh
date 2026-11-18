#!/usr/bin/env bash
# Exécute les tests des règles de nettoyage et conserve la sortie comme preuve.
set -euo pipefail
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PYSPARK_PYTHON="$HOME/venv-tp3/bin/python"
cd "$(dirname "$0")"
mkdir -p ../UC06_preuves/tests
"$HOME/venv-tp3/bin/python" test_regles.py 2>&1 | grep -vE "WARN|log4j|setLogLevel|incubator" | tee ../UC06_preuves/tests/resultats_tests_regles.txt
