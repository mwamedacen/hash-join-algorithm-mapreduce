package hashJoin;

import java.io.IOException;

public interface HashJoin {
	
	public void buildHashTable() throws IOException;;
	public void hashJoin() throws IOException;

}
