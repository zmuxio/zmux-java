package io.zmux;

import java.util.List;
import java.util.Objects;

/** The source and exact layout of a member opened by a capacity change. */
public record MemberSpec(String candidate, List<AttachmentSpec> attachments) {
    public MemberSpec {
        Objects.requireNonNull(candidate);
        attachments = List.copyOf(attachments);
        if (attachments.isEmpty()) throw new IllegalArgumentException("member has no attachment");
    }

    Directions directions() {
        boolean send = false, receive = false;
        for (AttachmentSpec attachment : attachments) {
            send |= attachment.directions().sends();
            receive |= attachment.directions().receives();
        }
        return send && receive ? Directions.BOTH : send ? Directions.SEND : Directions.RECEIVE;
    }
}
