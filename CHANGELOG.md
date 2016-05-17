## 3.0 [not released yet]
### Backward incompatible changes
- Producer partitioner is now implemented as a class and `Kafka.DefaultPartitioner` matches Java client implementation. Custom partitioners should inherit `Kafka.DefaultPartitioner`
- GroupConsumer assignment strategies are also now implemented as classes. Custom strategies should inherit from `Kafka.DefaultAssignmentStrategy`
- Using async compression now by default
### Added
- SSL support
