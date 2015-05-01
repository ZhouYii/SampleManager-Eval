import java.io.{IOException, ObjectOutputStream, ObjectInputStream}

import org.apache.hadoop.util.bloom.BloomFilter
import org.apache.hadoop.util.bloom.Key
import org.apache.hadoop.util.hash.Hash

import java.nio.ByteBuffer

class GSBloomFilter extends Serializable{

  @transient var filter = new BloomFilter()

  def setFilter(m: Int, k: Int, t: Int): Unit = {
    filter = new BloomFilter(m, k, t)
  }

  def add(id: Long): Unit = {
    val b = ByteBuffer.allocate(8).putLong(id)
    filter.add(new Key(b.array()))
  }

  def membershipTest(id: Long): Boolean = {
    val b = ByteBuffer.allocate(8).putLong(id)
    filter.membershipTest(new Key(b.array()))
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    filter.write(out)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    filter = new BloomFilter()
    filter.readFields(in)
  }
}
