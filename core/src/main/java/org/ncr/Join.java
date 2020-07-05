package org.ncr;

import java.util.Optional;
import java.util.function.BinaryOperator;

public class Join implements BinaryOperator<Optional<String>> {

    private final String separator;

    public Join(String separator) {
        this.separator = separator;
    }

    @Override
    public Optional<String> apply(Optional<String> first, Optional<String> second) {
        if (!first.isPresent()) {
            return second;
        } else if (!second.isPresent()) {
            return first;
        }
        return Optional.of(first.get() + separator + second.get());
    }
}
