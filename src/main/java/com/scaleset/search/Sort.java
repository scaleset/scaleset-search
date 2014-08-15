package com.scaleset.search;

public class Sort {

    public enum Direction {
        Ascending, Descending
    }

    public enum Type {
        Count, Lexical, None;
    }

    private Direction direction;

    private String field;

    private Type type = Type.Lexical;

    public static Sort lexicalDesc(String field) {
        return new Sort(field, Direction.Descending, Type.Lexical);
    }

    public static Sort lexicalAsc(String field) {
        return new Sort(field, Direction.Ascending, Type.Lexical);
    }

    public static Sort countDesc(String field) {
        return new Sort(field, Direction.Descending, Type.Count);
    }

    public static Sort countAsc(String field) {
        return new Sort(field, Direction.Ascending, Type.Count);
    }

    protected Sort() {
    }

    public Sort(String field, Direction direction) {
        this.field = field;
        this.direction = direction;
    }

    public Sort(String field, Direction direction, Type type) {
        this.field = field;
        this.direction = direction;
        this.type = type;
    }

    public Sort(Direction direction, Type type) {
        this.direction = direction;
        this.type = type;
    }

    public Direction getDirection() {
        return direction;
    }

    public String getField() {
        return field;
    }

    public Type getType() {
        return type;
    }

}
