package stream.schema;

public class StreamsEnum {

    public enum OpType {
        INSERT("I"), UPDATE("U"), DELETE("D"), PUT("P"),RELATION("R"), OTHER("O");

        private String name;

        private OpType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
