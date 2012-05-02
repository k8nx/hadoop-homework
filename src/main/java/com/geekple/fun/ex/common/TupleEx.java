package com.geekple.fun.ex.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author Daegeun Kim
 */
public class TupleEx implements Writable {
	/**
	 * 반복적인 FieldSchema 분석을 줄이기위해 캐쉬
	 */
	protected static final Map<String, FieldSchema> CACHED = new HashMap<String, FieldSchema>();
	
	protected String alias;
	protected Writable[] values;
	protected Map<String, Writable> valueMap;
	
	public static TupleEx create(String alias) {
		TupleEx tuple = new TupleEx();
		tuple.alias = alias;
		return tuple;
	}

	public static TupleEx create(String alias, Text value) {
		TupleEx tuple = new TupleEx();
		tuple.alias = alias;
		tuple.setValuesFromText(value);
		return tuple;
	}

	public static void create(TupleEx value, Text line) {
		value.setValuesFromText(line);
	}
	
	/**
	 * 필드 스키마 등록. 등록된 경우는 무시
	 * @param alias
	 * @param definition
	 */
	public static void registerFieldSchema(String alias, String definition) {
		if (alias != null && !CACHED.containsKey(alias) && definition != null) {
			CACHED.put(alias, FieldSchema.parse(definition));
		}
	}
	
	/**
	 * alias 에 해당하는 필드스키마 반환. NULL 반환하지않고 NULL 용 객체 반환.
	 * @param alias
	 * @return
	 */
	static FieldSchema getFieldSchema(String alias) {
		return CACHED.containsKey(alias) ? CACHED.get(alias) : FieldSchema.NULL;
	}
	
	public TupleEx() {
	}
	
	public TupleEx(String alias, Writable[] values) {
		this.alias = alias;
		this.values = values;
		sync();
	}

	@Override
	public String toString() {
		boolean first = true;
		StringBuilder sb = new StringBuilder();
		for (Writable value : values) {
			sb.append(first ? "" : "\t").append(value);
			first = false;
		}
		return sb.toString();
	}
	
	public final int size() {
		return values == null ? 0 : values.length;
	}
	
	/**
	 * 등록된 스키마와 values 배열에 들어있는 값과 valueMap 동기화
	 */
	protected final void sync() {
		if (valueMap == null) {
			valueMap = new HashMap<String, Writable>();
		}
		FieldSchema schema = getFieldSchema(alias);
		int current = 0;
		for (Field field : schema) {
			if (values.length > current) {
				valueMap.put(field.name, values[current]);
			}
			current++;
		}
	}
	
	/**
	 * 요청한 필드명과 일치하는 value 를 반환. $로 시작하는 경우는 index 로 처리
	 * @param fieldName
	 * @return
	 */
	public final Writable getValue(String fieldName) {
		if (valueMap == null) {
			return null;
		}
		if (fieldName.startsWith("$")) {
			try {
				return values[Integer.parseInt(fieldName.substring(1))];
			} catch (Exception e) {
				return null;
			}
		} else {
			return valueMap.get(fieldName);
		}
	}
	
	/**
	 * 필드명과 일치하는 value 값 변경
	 * @param fieldName
	 * @param value
	 */
	public final void setValue(String fieldName, Writable value) {
		if (fieldName.startsWith("$")) {
			try {
				values[Integer.parseInt(fieldName.substring(1))] = value;
				valueMap.put(fieldName, value);
			} catch (Exception e) {
			}
		} else {
			values[getFieldSchema(alias).getFieldIndex(fieldName)] = value;
			valueMap.put(fieldName, value);
		}
	}
	
	public final void setValuesFromText(Text value) {
		if (value == null) {
			return;
		}
		String literal = value.toString();
		if (literal == null || literal.length() == 0) {
			return;
		}
		StringTokenizer tokens = new StringTokenizer(literal);
		int current = 0;
		FieldSchema schema = getFieldSchema(alias);
		values = new Writable[schema.size()];
		for (Field field : schema) {
			if (tokens.hasMoreTokens()) {
				values[current] = field.invoke(tokens.nextToken());
			} else {
				break;
			}
			current++;
		}
		sync();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		String alias = WritableUtils.readString(in);
		FieldSchema schema = getFieldSchema(alias);
		Writable[] values = new Writable[schema.size()];
		int current = 0;
		for (Field field : schema) {
			values[current++] = field.invoke(in);
		}
		this.alias = alias;
		this.values = values;
		sync();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, alias);
		for (Writable value : values) {
			value.write(out);
		}
	}
	
	static class FieldSchema implements Iterable<Field>, Writable {
		static final FieldSchema NULL = new FieldSchema();
		
		private String cached;
		
		List<Field> fields;
		Map<String, Field> fieldMap;
		Map<String, Integer> fieldIndex;
		
		FieldSchema() {
			fields = new ArrayList<Field>();
			fieldMap = new HashMap<String, Field>();
			fieldIndex = new HashMap<String, Integer>();
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			// mapper 에서 serialization 할 때 많이 호출이 될 텐데 반복적인 요청에 일일이 대응하지 않고 캐쉬
			// cache 에서 evict 할 일은 거의 없어서 그 부분은 생략
			if (cached == null) {
				StringBuilder sb = new StringBuilder();
				boolean first = true;
				for (Field field : fields) {
					sb.append(first ? "" : ",").append(field.name).append(":").append(field.type.name().toLowerCase());
					first = false;
				}
				cached = sb.toString();
			}
			WritableUtils.writeString(out, cached);
		}
		
		@Override
		public Iterator<Field> iterator() {
			return fields.iterator();
		}
		
		static FieldSchema parse(String definition) {
			FieldSchema schema = new FieldSchema();
			
			int index = 0;
			for (String field : definition.split(",")) {
				String[] pair = field.replaceAll(" ", "").split(":");
				if (pair.length == 0 || pair.length > 2) {
					throw new IllegalFieldDefinitionException(field);
				}
				if (pair.length == 2) {
					try {
						Field.Type type = Field.Type.valueOf(pair[1].toUpperCase());
						Field f = new Field(index, pair[0], type);
						schema.fields.add(f);
						schema.fieldMap.put(pair[0], f);
					} catch (IllegalArgumentException e) {
						throw new UnknownFieldException(pair[0] + " " + pair[1]);
					}
				} else {
					Field f = new Field(index, pair[0]);
					schema.fields.add(f);
					schema.fieldMap.put(pair[0], f);
				}
				index++;
			}
			return schema;
		}
		
		public int getFieldIndex(String fieldName) {
			for (int i = 0; i < fields.size(); i++) {
				if (fieldName.equals(fields.get(i).name)) {
					return i;
				}
			}
			throw new UnknownFieldException(fieldName);
		}
		
		public Field getField(int index) {
			return fields.get(index);
		}
		
		public Field getField(String fieldName) {
			if (fieldName.startsWith("$")) {
				try {
					return getField(Integer.parseInt(fieldName.substring(1)));
				} catch (Exception e) {
				}
			}
			return fieldMap.get(fieldName);
		}
		
		public List<Field> getFieldSchema() {
			return Collections.unmodifiableList(fields);
		}
		
		public int size() {
			return fields.size();
		}
	}
	
	TupleEx merge(String alias, TupleEx other) {
		TupleEx tuple = new TupleEx();
		tuple.alias = alias == null ? this.alias : alias;
		FieldSchema schema = getFieldSchema(tuple.alias);
		if (values == null) {
			tuple.values = new Writable[schema.fields.size()];
		} else {
			tuple.values = values;
		}
		tuple.sync();
		for (Field field : schema) {
			Writable value = other.getValue(field.name);
			if (value != null) {
				tuple.setValue(field.name, value);
			}
		}
		return tuple;
	}
	
	TupleEx export(String[] names) {
		TupleEx tuple = new TupleEx();
		tuple.alias = alias;
		FieldSchema schema = TupleEx.getFieldSchema(alias);
		Writable[] values = new Writable[schema.fields.size()];
		for (int i = 0; i < names.length; i++) {
			values[schema.getFieldIndex(names[i])] = getValue(names[i]);
		}
		tuple.values = values;
		tuple.sync();
		return tuple;
	}

	@SuppressWarnings("rawtypes")
	Key exportKey(String[] names) {
		WritableComparable[] values = new WritableComparable[names.length];
		for (int i = 0; i < names.length; i++) {
			Writable value = getValue(names[i]);
			if (value instanceof WritableComparable) {
				values[i] = (WritableComparable) value;
			} else {
				throw new IllegalStateException("must be instance of WritableComparable - " + names[i]);
			}
		}
		return new Key(alias, values);
	}


	static interface FieldConverter {
		Class<? extends Writable> supportedType();
		
		Writable convert(String value);

		Writable convert(DataInput in);
	}
	
	static class FieldConverterAdapter implements FieldConverter {
		private Class<? extends Writable> supportedType;
		
		public FieldConverterAdapter(Class<? extends Writable> supportedType) {
			this.supportedType = supportedType;
		}

		@Override
		public Class<? extends Writable> supportedType() {
			return supportedType;
		}
		
		@Override
		public Writable convert(String value) {
			return null;
		}
		
		@Override
		public Writable convert(DataInput in) {
			Writable value = convert((String) null);
			try {
				value.readFields(in);
			} catch (IOException e) {
			}
			return value;
		}
	}
	
	public static class Field {
		static final Map<Type, FieldConverter> converters = new HashMap<Type, FieldConverter>();
		static final Map<Type, Class<? extends Writable>> writableMap = new HashMap<Type, Class<? extends Writable>>();
		
		static {
			registerBuiltInConverter();
		}
		
		public static void registerType(Type type, FieldConverter converter) {
			converters.put(type, converter);
			writableMap.put(type, converter.supportedType());
		}

		static enum Type {
			CHARARRAY, INT, FLOAT;
		}

		int index;
		String name;
		Type type;
		
		Field(int index, String name) {
			this(index, name, Type.CHARARRAY);
		}
		
		Field(int index, String name, Type type) {
			this.index = index;
			this.name = name;
			this.type = type;
		}
		
		Writable invoke(String value) {
			if (converters.containsKey(type)) {
				return converters.get(type).convert(value);
			}
			return null;
		}

		public Writable invoke(DataInput in) {
			if (converters.containsKey(type)) {
				return converters.get(type).convert(in);
			}
			return null;
		}
		
		/**
		 * 기본적으로 사용할 변환 클래스 등록
		 */
		private static void registerBuiltInConverter() {
			// BUILT-IN
			registerType(Type.CHARARRAY, new FieldConverterAdapter(Text.class) {
				@Override
				public Writable convert(String value) {
					try {
						return new Text(value);
					} catch (Exception e) {
						return new Text();
					}
				}
			});
			registerType(Type.INT, new FieldConverterAdapter(LongWritable.class) {
				@Override
				public Writable convert(String value) {
					try {
						return new LongWritable(Long.parseLong(value));
					} catch (Exception e) {
						return new LongWritable();
					}
				}
			});
			registerType(Type.FLOAT, new FieldConverterAdapter(DoubleWritable.class) {
				@Override
				public Writable convert(String value) {
					try {
						return new DoubleWritable(Double.parseDouble(value));
					} catch (Exception e) {
						return new DoubleWritable();
					}
				}
			});
		}
	}
	
	static class IllegalFieldDefinitionException extends RuntimeException {
		private static final long serialVersionUID = 7170323617518806457L;

		IllegalFieldDefinitionException(String message) {
			super(message);
		}
	}
	
	static class UnknownFieldException extends RuntimeException {
		private static final long serialVersionUID = 7707050448621791747L;

		UnknownFieldException(String fieldName) {
			super(fieldName);
		}
	}
}
