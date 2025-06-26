# EDC Technology HuaweiCloud

[![documentation](https://img.shields.io/badge/documentation-8A2BE2?style=flat-square)](https://eclipse-edc.github.io)
[![discord](https://img.shields.io/badge/discord-chat-brightgreen.svg?style=flat-square&logo=discord)](https://discord.gg/n4sD9qtjMQ)
[![latest version](https://img.shields.io/maven-central/v/org.eclipse.edc.huawei/obs-core?logo=apache-maven&style=flat-square&label=latest%20version)](https://search.maven.org/artifact/org.eclipse.edc.huawei/obs-core)
[![license](https://img.shields.io/github/license/eclipse-edc/Technology-Azure?style=flat-square&logo=apache)](https://www.apache.org/licenses/LICENSE-2.0)
<br>
[![build](https://img.shields.io/github/actions/workflow/status/eclipse-edc/Technology-HuaweiCloud/verify.yaml?branch=main&logo=GitHub&style=flat-square&label=ci)](https://github.com/eclipse-edc/Technology-HuaweiCloud/actions/workflows/verify.yaml?query=branch%3Amain)
[![snapshot build](https://img.shields.io/github/actions/workflow/status/eclipse-edc/Technology-HuaweiCloud/trigger_snapshot.yml?branch=main&logo=GitHub&style=flat-square&label=snapshot-build)](https://github.com/eclipse-edc/Technology-HuaweiCloud/actions/workflows/trigger_snapshot.yml)
[![nightly build](https://img.shields.io/github/actions/workflow/status/eclipse-edc/Technology-HuaweiCloud/nightly.yml?branch=main&logo=GitHub&style=flat-square&label=nightly-build)](https://github.com/eclipse-edc/Technology-HuaweiCloud/actions/workflows/nightly.yml)

---

Contains implementations for several Huawei Cloud services, such as OBS and GaussDB

## Documentation

Base documentation can be found on the [documentation website](https://eclipse-edc.github.io).

### Build:
```shell
./gradlew shadowJar
```

### Run
```shell
java -jar launchers/huawei-cloud-runtime/build/libs/hds-connector.jar
```
## Contributing

See [how to contribute](https://github.com/eclipse-edc/eclipse-edc.github.io/blob/main/CONTRIBUTING.md).
