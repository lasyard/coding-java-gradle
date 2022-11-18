package io.github.lasyard.calcite;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableScan;
import org.immutables.value.Value;

import javax.annotation.Nonnull;

@Slf4j
@Value.Enclosing
public class MockTableScanRule extends RelRule<MockTableScanRule.Config> {
    public static final MockTableScanRule INSTANCE = MockTableScanRule.Config.DEFAULT.toRule();

    public MockTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        MockTableScan scan = call.rel(0);
        // Actually nothing changed.
        call.transformTo(scan);
        log.info("Transformed.");
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableMockTableScanRule.Config.builder()
            .operandSupplier(b0 -> b0.operand(TableScan.class).noInputs())
            .build();

        @Override
        default MockTableScanRule toRule() {
            return new MockTableScanRule(this);
        }
    }
}
