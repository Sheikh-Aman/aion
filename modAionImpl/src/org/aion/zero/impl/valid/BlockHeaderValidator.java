package org.aion.zero.impl.valid;

import java.util.LinkedList;
import java.util.List;

import org.aion.mcf.blockchain.BlockHeader;
import org.slf4j.Logger;

public class BlockHeaderValidator extends AbstractBlockHeaderValidator {

    private List<BlockHeaderRule> rules;

    public BlockHeaderValidator(List<BlockHeaderRule> rules) {
        this.rules = rules;
    }

    public boolean validate(final BlockHeader header, final Logger logger) {
        List<RuleError> errors = new LinkedList<>();
        for (BlockHeaderRule rule : rules) {
            if (!rule.validate(header, errors)) {
                if (logger != null) logErrors(logger, errors);
                return false;
            }
        }
        return true;
    }
}
