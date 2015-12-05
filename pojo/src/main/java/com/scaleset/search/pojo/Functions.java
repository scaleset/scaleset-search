package com.scaleset.search.pojo;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scaleset.utils.Coerce;
import org.mvel2.MVEL;
import org.mvel2.integration.PropertyHandlerFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class Functions {

    static {
        PropertyHandlerFactory.registerPropertyHandler(ObjectNode.class, new JsonNodePropertyHandler());
    }

    public static Predicate all(final List<Predicate> predicates) {
        Predicate result = (obj) -> {
            for (Predicate p : predicates) {
                if (!p.test(obj)) {
                    return false;
                }
            }
            return true;
        };
        return result;
    }

    public static Predicate any(final List<Predicate> predicates) {
        Predicate result = (obj) -> {
            for (Predicate p : predicates) {
                if (p.test(obj)) {
                    return true;
                }
            }
            return false;
        };
        return result;
    }

    public static Predicate matches(String path, Pattern pattern) {
        Function<Object, List> fieldGetter = field(path);
        return (obj) -> {
            boolean result = false;
            List values = fieldGetter.apply(obj);
            for (Object value : values) {
                if (value != null) {
                    result = pattern.matcher(Coerce.toString(value)).matches();
                }
                if (result) break;
            }
            return result;
        };
    }

    public static Predicate not(Predicate predicate) {
        return obj -> {
            return !predicate.test(obj);
        };
    }

    protected static List<Object> asList(Object obj) {
        if (obj == null) {
            return Collections.emptyList();
        } else if (obj instanceof List) {
            return (List) obj;
        } else if (obj.getClass().isArray()) {
            int length = Array.getLength(obj);
            List result = new ArrayList<>(length);
            for (int i = 0; i < length; ++i) {
                Object item = Array.get(obj, i);
                result.add(item);
            }
            return result;
        } else {
            return Arrays.asList(obj);
        }
    }

    public static Predicate term(String path, final Object term) {
        Function<Object, List> fieldGetter = field(path);
        return (obj) -> {
            List values = fieldGetter.apply(obj);
            boolean result = false;
            for (Object value : values) {
                if (value instanceof Number) {
                    Object termVal = Coerce.to(term, value.getClass());
                    result = termVal.equals(value);
                } else if (value != null) {
                    String termStr = term.toString().toLowerCase();
                    result = value.toString().toLowerCase().contains(termStr);
                }
                if (result) break;
            }
            return result;
        };
    }

    public static Predicate lte(String path, final Object term) {
        String termStr = term.toString().toLowerCase();
        Function<Object, List> fieldGetter = field(path);
        return (obj) -> {
            boolean result = false;
            List values = fieldGetter.apply(obj);
            for (Object value : values) {
                if (value instanceof Comparable) {
                    Object termVal = Coerce.to(term, value.getClass());
                    result = ((Comparable) value).compareTo(termVal) <= 0;
                } else if (value != null) {
                    result = value.toString().compareTo(termStr) <= 0;
                }
                if (result) break;
            }
            return result;
        };
    }

    public static Predicate gte(String path, final Object term) {
        String termStr = term.toString().toLowerCase();
        Function<Object, List> fieldGetter = field(path);
        return (obj) -> {
            boolean result = false;
            List values = fieldGetter.apply(obj);
            for (Object value : values) {
                if (value instanceof Comparable) {
                    Object termVal = Coerce.to(term, value.getClass());
                    result = ((Comparable) value).compareTo(termVal) >= 0;
                } else if (value != null) {
                    result = value.toString().compareTo(termStr) >= 0;
                }
                if (result) break;
            }
            return result;
        };
    }

    public static Predicate gt(String path, final Object term) {
        String termStr = term.toString().toLowerCase();
        Function<Object, List> fieldGetter = field(path);
        return (obj) -> {
            boolean result = false;
            List values = fieldGetter.apply(obj);
            for (Object value : values) {
                if (value instanceof Comparable) {
                    Object termVal = Coerce.to(term, value.getClass());
                    result = ((Comparable) value).compareTo(termVal) > 0;
                } else if (value != null) {
                    result = value.toString().compareTo(termStr) > 0;
                }
                if (result) break;
            }
            return result;
        };
    }

    public static Predicate lt(String path, final Object term) {
        String termStr = term.toString().toLowerCase();
        Function<Object, List> fieldGetter = field(path);
        return (obj) -> {
            boolean result = false;
            List values = fieldGetter.apply(obj);
            for (Object value : values) {
                if (value instanceof Comparable) {
                    Object termVal = Coerce.to(term, value.getClass());
                    result = ((Comparable) value).compareTo(termVal) < 0;
                } else if (value != null) {
                    result = value.toString().compareTo(termStr) < 0;
                }
                if (result) break;
            }
            return result;
        };
    }

    public static Function<Object, List> field(String field) {
        return (obj) -> {
            try {
                return asList(MVEL.eval(field, obj));
            } catch (Throwable e) {
                return Collections.emptyList();
            }
        };
    }

}
