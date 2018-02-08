package myPractice.JVM.GC;


public class ReferenceGC
{
    final int Mkbs = 1<<20;

    byte [] bigData = new byte[10*Mkbs];
    Object referenceObj;

    public static void main(String[] args)
    {
        ReferenceGC r1 = new ReferenceGC();
        ReferenceGC r2 = new ReferenceGC();
        r1.referenceObj = r2;
        r2.referenceObj = r1;

        r1=null;
        r2=null;
        System.gc();
//      [GC (System.gc()) [PSYoungGen: 23142K->696K(38400K)] 23142K->704K(125952K), 0.0011210 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//      [Full GC (System.gc()) [PSYoungGen: 696K->0K(38400K)] [ParOldGen: 8K->578K(87552K)] 704K->578K(125952K), [Metaspace: 2856K->2856K(1056768K)], 0.0039528 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
    }

}
