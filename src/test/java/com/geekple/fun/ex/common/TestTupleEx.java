package com.geekple.fun.ex.common;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

import java.util.List;

import com.geekple.fun.Description;
import com.geekple.fun.ByteArrayDataOutput;
import com.geekple.fun.TestCaseSupport;
import com.geekple.fun.ex.common.TupleEx.Field;
import com.geekple.fun.ex.common.TupleEx.FieldSchema;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

/**
 * TupleEx 클래스 검증 테스트 클래스
 * 
 * @author Daegeun Kim
 */
public class TestTupleEx extends TestCaseSupport {
	private static final String EMP_DEFINITION = "empno:int, name:chararray, job, mgr, hiredate, sal:int, comm, deptno:int";
	private static final String DEPT_DEFINITION = "deptno:int, name:chararray, location:chararray";

	@Test
	@Description("FieldSchema 분석 테스트")
	public void testFieldSchemaParsing() throws Exception {
		FieldSchema schema = FieldSchema.parse(EMP_DEFINITION);
		List<Field> fields = schema.getFieldSchema();
		
		Assert.assertThat(fields.size(), is(8));
		Assert.assertThat(composeFieldNames(fields), is("empno name job mgr hiredate sal comm deptno"));
		Assert.assertThat(composeFieldTypes(fields), is("int chararray chararray chararray chararray int chararray int"));
	}
	
	@Test(expected = TupleEx.IllegalFieldDefinitionException.class)
	@Description("IllegalFieldDefinitionException 발생 테스트")
	public void testSchemaParsing_throwIllegalFieldDefinitionException() throws Exception {
		FieldSchema.parse("name: chararray, age:a:a");
	}
	
	@Test(expected = TupleEx.UnknownFieldException.class)
	@Description("UnknownFieldException 발생 테스트")
	public void testSchemaParsing_throwUnknownFieldException() throws Exception {
		FieldSchema.parse("name: chararray, age:asdf");
	}
	
	@Test
	public void testMerge() throws Exception {
		TupleEx.registerFieldSchema("A", "name:chararray");
		
		TupleEx other = new TupleEx("A", new Writable[] { new Text("DGKIM") });
		TupleEx merged = TupleEx.create("A").merge(null, other);
		System.out.println(merged);
	}

	@Test
	@Description("Serialization/Descrialization 정상동작하는지 검사")
	public void testReadWrite() throws Exception {
		TupleEx.registerFieldSchema("a", EMP_DEFINITION);
		TupleEx tuple1 = TupleEx.create("a", new Text("7369 SMITH CLERK 7902 1980-12-17 800 NULL 20"));

		ByteArrayDataOutput out = writeToStream(tuple1);

		TupleEx newtuple = new TupleEx();
		newtuple.readFields(readFromStream(out));

		assertTupleValueWithComposedValue(newtuple, "7369 SMITH CLERK 7902 1980-12-17 800 NULL 20");
	}

	/**
	 * INT 에 해당하는 빌트인 필드변환기는 LongWritable 를 반환한다. 여기서 테스트는 IntWritable 로 덮어쓰고 IntWritable 로 반환하는지 테스트
	 * @throws Exception
	 */
	@Test
	@Description("필드 변환기 등록 검사")
	public void testRegisterFieldConverter() throws Exception {
		TupleEx.Field.registerType(TupleEx.Field.Type.INT, new TupleEx.FieldConverterAdapter(IntWritable.class) {
			@Override
			public Writable convert(String value) {
				return new IntWritable(Integer.parseInt(value));
			}
		});
		TupleEx.registerFieldSchema("a", EMP_DEFINITION);
		TupleEx tuple1 = TupleEx.create("a", new Text("7369 SMITH CLERK 7902 1980-12-17 800 NULL 20"));
		
		Writable emploeeNo = tuple1.getValue("empno");
		Assert.assertThat(emploeeNo, notNullValue());
		Assert.assertTrue(emploeeNo instanceof IntWritable);
		Assert.assertThat(((IntWritable) emploeeNo).get(), is(7369));
	}

	@Test
	@Description("필드 스키마 등록 후 TupleEx 생성한 다음 값이 같은 값인지 검사")
	public void testTuple() throws Exception {
		TupleEx.registerFieldSchema("a", EMP_DEFINITION);
		TupleEx.registerFieldSchema("b", DEPT_DEFINITION);
		TupleEx tuple1 = TupleEx.create("a", new Text("7369 SMITH CLERK 7902 1980-12-17 800 NULL 20"));
		TupleEx tuple2 = TupleEx.create("b", new Text("10 ACCOUNTING NEWYORK"));
		
		assertTupleValueWithComposedValue(tuple1, "7369 SMITH CLERK 7902 1980-12-17 800 NULL 20");
		assertTupleValueWithComposedValue(tuple2, "10 ACCOUNTING NEWYORK");
	}
	
	/**
	 * Tuple 이 가진 값을 공백구분한 값으로 변환 후 같은 값인지 검사
	 * @param tuple
	 * @param value 공백으로 구분한 값
	 */
	private void assertTupleValueWithComposedValue(TupleEx tuple, String value) {
		FieldSchema schema = TupleEx.getFieldSchema(tuple.alias);
		StringBuilder sb = new StringBuilder();
		for (TupleEx.Field field : schema) {
			sb.append(tuple.getValue(field.name)).append(" ");
		}
		Assert.assertThat(sb.toString().trim(), is(value));
	}

	/**
	 * 필드 명을 공백구분값으로 병합"
	 * @param fields
	 * @return 공백으로 구분한 값
	 */
	private String composeFieldNames(List<Field> fields) {
		StringBuilder sb = new StringBuilder();
		for (Field field : fields) {
			sb.append(field.name).append(" ");
		}
		return sb.toString().trim();
	}

	/**
	 * 필드 타입값을 공백구분값으로 병합
	 * @param fields
	 * @return 공백으로 구분한 값
	 */
	private String composeFieldTypes(List<Field> fields) {
		StringBuilder sb = new StringBuilder();
		for (Field field : fields) {
			sb.append(field.type.name().toLowerCase()).append(" ");
		}
		return sb.toString().trim();
	}
}
