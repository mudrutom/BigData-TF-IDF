package cz.cvut.bigdata.tfidf.terms;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class TermFrequencyTest {

	private static final String text =
			"<h2>Cíle lingvistiky</h2> <li> Deskriptivní lingvistika. Cílem deskriptivní lingvistiky je popsat jazykový systém" +
			"(např. výraz \"hladový vlk\" odkazuje k objektu, který je \"hladový\" a který je \"vlk\")" +
			"Bohuslav Hála v článku \"Význam experimentálně-fonetického zkoumání nářečí\" in \"Listy filologické\", 1940.</ref>)" +
			"x1 + A2a * b_8 =100";

	private static final List<String> textProcessed = Arrays.asList(
			"cíl", "lingvistik", "deskriptivn", "lingvistik", "cíl",  "deskriptivn", "lingvistik", "pops", "jazyk", "syst",
			"např", "výrah", "hlad", "vlk", "odkazuj", "objekt", "hlad", "vlk",
			"bohuslav", "hál", "význam", "experimentáln", "fonetick", "zkoumán", "nářek", "in", "list", "filologick"
		);

	private TermFrequencyMapper mapper;

	@Before
	public void setup() {
		mapper = new TermFrequencyMapper();
	}

	@After
	public void cleanup() {
		mapper = null;
	}

	@Test
	public void testTermParsing() throws IOException {
		final List<String> result = mapper.parseTerms(text);
		assertEquals(textProcessed, result);
	}

}
