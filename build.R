# TODO: use environment variables instead of the hard coded versions
spec <- sparklyr:::spark_compilation_spec(
      spark_version = "2.3.4",
      spark_home = "/spark",
      scalac_path = "scalac",
      jar_name = "sparklyr-2.3-2.11.8.jar",
      jar_dep = "/opt/graalvm-ce-java8-19.3.0/jre/lib/boot/graal-sdk.jar",
      scala_filter = sparklyr:::make_version_filter("2.3.4")
    )
sparklyr:::compile_package_jars(spec = spec)
