package example.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class TestDeserializedOk implements Externalizable {

    private static final long serialVersionUID = 2L;

    public static volatile boolean isDeserialized = false;
    public static volatile boolean isSerialized = false;

    private int i = 0;

    public TestDeserializedOk() {
    }

    public TestDeserializedOk(int i) {
        this.i = i;
    }

    public int getI() {
        return i;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        isSerialized = true;
        out.writeInt(i);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        isDeserialized = true;
        this.i = in.readInt();
    }
}
