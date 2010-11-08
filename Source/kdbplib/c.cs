﻿// kdb+ provider from kx. 
// taken from https://code.kx.com/trac/browser/kx/kdb%2B/c/c.cs.
// There may be newer version available.
// Minor changes:
// 1. Added constructor to accept buffer size limit.
// 2. Added c.NULL(Type t) method.
// 3. Types Date, Month, Minute, Second, KTimeSpan were made serializable. 
// Also default constructors were added for xml serialization.
// 4. Types Date, Month, Minute, Second, KTimeSpan now implement IComparable.
namespace kx
{
    //2010.08.05 Added KException for exceptions due to server error, authentication fail and func decode
    //2010.01.14 Exposed static var e (Encoding) as public
    //2010.01.12 Added support for unicode encoding, defaults to ASCII 
    //2010.01.11 Added null checks for qn for UDTs Date,Month,Minute,Second,KTimespan
    //2010.01.04 Added new time types (timespan->KTimespan,timestamp->DateTime), drop writing kdb+ datetime
    //2009.10.19 Limit reads to blocks of 64kB to reduce load on kernel memory
    //2007.09.26 0Wz to MaxValue
    //2006.10.05 truncate string at null
    //2006.09.29 NULL  c.Date class(sync with c.java)
    using System;
    using System.IO;
    using System.Net.Sockets; //csc c.cs  given >q trade.q -p 5001
    
        public class c : TcpClient
        {
            /*public static void Main(string[]args){
            //c.ReceiveTimeout=1000;
            //c c=new c("localhost",5010);c.k(".u.sub[`trade;`MSFT.O`IBM.N]");while(true){object r=c.k();O(n(at(r,2)));}
             c c=new c("localhost",5001);
            //c.e=System.Text.Encoding.UTF8;
            //O("Unicode "+c.k("`$\"c\"$0x52616e627920426ac3b6726b6c756e64204142"));                                
            //object[]x=new object[4];x[0]=t();x[1]="xx";x[2]=(double)93.5;x[3]=300;tm();for(int i=0;i<1000;++i)c.k("insert","trade",x);tm();
            //Flip r=td(c.k("select sum price by sym from trade"));O("cols: "+n(r.x));O("rows: "+n(r.y[0]));
             c.Close();
            }*/

            private const int DefaultMaxBufferSize = 65536;
            public static System.Text.Encoding e = System.Text.Encoding.ASCII;
            private readonly int _maxBufferSize = DefaultMaxBufferSize;
            private byte[] b, B;
            private int j, J;
            private bool a;
            private Stream s;

            public new void Close()
            {
                s.Close();
                base.Close();
            }

            public c(string h, int p) : this(h, p, Environment.UserName)
            {
            }

            public c(string h, int p, string u)
                : this(h, p, u, DefaultMaxBufferSize)
            {
            }

            public c(string h, int p, string u, int maxBufferSize)
            {
                _maxBufferSize = maxBufferSize;
                Connect(h, p);
                s = this.GetStream();
                B = new byte[2 + u.Length];
                J = 0;
                w(u + "\x1");
                s.Write(B, 0, J);
                if (1 != s.Read(B, 0, 1))
                {
                    B = new byte[1 + u.Length];
                    Connect(h, p);
                    s = this.GetStream();
                    J = 0;
                    w(u);
                    s.Write(B, 0, J);
                    if (1 != s.Read(B, 0, 1)) throw new KException("access");
                }
            }

            private static int ns(string s)
            {
                int i = s.IndexOf('\0');
                i = -1 < i ? i : s.Length;
                return e.GetBytes(s.Substring(0, i)).Length;
            }

            private static TimeSpan t()
            {
                return DateTime.Now.TimeOfDay;
            }

            private static TimeSpan v;

            private static void tm()
            {
                TimeSpan u = v;
                v = t();
                O(v - u);
            }

            private static void O(object x)
            {
                Console.WriteLine(x);
            }

            private static string i2(int i)
            {
                return String.Format("{0:00}", i);
            }

            private static int ni = Int32.MinValue;
            private static long nj = Int64.MinValue, o = (long) 8.64e11*730119;
            private static double nf = Double.NaN;

            private static object[] NU = {
                                             null, false, null, null, (byte) 0, Int16.MinValue, ni, nj, (Single) nf, nf,
                                             ' ', "", new DateTime(0),
                                             new Month(ni), new Date(ni), new DateTime(0), new KTimespan(nj),
                                             new Minute(ni), new Second(ni), new TimeSpan(ni)
                                         };

            private static object NULL(char c)
            {
                return NU[" b  xhijefcspmdznuvt".IndexOf(c)];
            }

            public static object NULL(Type t)
            {
                int index = -GetIndexForType(t);
                return NU[index];
            }

            public static bool qn(object x)
            {
                int t = -c.t(x);
                return t > 4 && x.Equals(NU[t]);
            }

            private void u()
            {
                int n = 0, r = 0, f = 0, s = 8, p = s;
                short i = 0;
                j = 0;
                byte[] dst = new byte[ri()];
                int d = j;
                int[] aa = new int[256];
                while (s < dst.Length)
                {
                    if (i == 0)
                    {
                        f = 0xff & (int) b[d++];
                        i = 1;
                    }
                    if ((f & i) != 0)
                    {
                        r = aa[0xff & (int) b[d++]];
                        dst[s++] = dst[r++];
                        dst[s++] = dst[r++];
                        n = 0xff & (int) b[d++];
                        for (int m = 0; m < n; m++) dst[s + m] = dst[r + m];
                    }
                    else dst[s++] = b[d++];
                    while (p < s - 1) aa[(0xff & (int) dst[p]) ^ (0xff & (int) dst[p + 1])] = p++;
                    if ((f & i) != 0) p = s += n;
                    i *= 2;
                    if (i == 256) i = 0;
                }
                b = dst;
                j = 8;
            }

            private static DateTime za = DateTime.MinValue, zw = DateTime.MaxValue;

            [Serializable]
            public class Date : IComparable
            {
                public int i;

                private Date()
                {
                }

                public Date(int x)
                {
                    i = x;
                }

                public DateTime DateTime()
                {
                    return i == -int.MaxValue
                               ? za
                               : i == int.MaxValue ? zw : new DateTime(i == ni ? 0L : (long) 8.64e11*i + o);
                }

                public Date(long x)
                {
                    i = x == 0L ? ni : (int) (x/(long) 8.64e11) - 730119;
                }

                public Date(DateTime z) : this(z.Ticks)
                {
                }

                public override string ToString()
                {
                    return i == ni ? "" : this.DateTime().ToString("d");
                }

                public override bool Equals(object o)
                {
                    if (o == null) return false;
                    if (this.GetType() != o.GetType()) return false;
                    Date d = (Date) o;
                    return i == d.i;
                }

                public override int GetHashCode()
                {
                    return i;
                }

                public int CompareTo(object obj)
                {
                    if (obj == null)
                        return 1;
                    
                    Date other = obj as Date;
                    if (other == null)
                        return 1;

                    return i.CompareTo(other.i);
                }
            }

            [Serializable]
            public class Month:IComparable
            {
                public int i;

                private Month()
                {
                }

                public Month(int x)
                {
                    i = x;
                }

                public override string ToString()
                {
                    int m = 24000 + i, y = m/12;
                    return i == ni ? "" : i2(y/100) + i2(y%100) + "-" + i2(1 + m%12);
                }

                public override bool Equals(object o)
                {
                    if (o == null) return false;
                    if (this.GetType() != o.GetType()) return false;
                    Month m = (Month) o;
                    return i == m.i;
                }

                public override int GetHashCode()
                {
                    return i;
                }

                public int CompareTo(object obj)
                {
                    if (obj == null)
                        return 1;

                    Month other = obj as Month;
                    if (other == null)
                        return 1;

                    return i.CompareTo(other.i);
                }
            }

            [Serializable]
            public class Minute:IComparable
            {
                public int i;

                private Minute()
                {
                }

                public Minute(int x)
                {
                    i = x;
                }

                public override string ToString()
                {
                    return i == ni ? "" : i2(i/60) + ":" + i2(i%60);
                }

                public override bool Equals(object o)
                {
                    if (o == null) return false;
                    if (this.GetType() != o.GetType()) return false;
                    Minute m = (Minute) o;
                    return i == m.i;
                }

                public override int GetHashCode()
                {
                    return i;
                }

                public int CompareTo(object obj)
                {
                    if (obj == null)
                        return 1;

                    Minute other = obj as Minute;
                    if (other == null)
                        return 1;

                    return i.CompareTo(other.i);
                }
            }

            [Serializable]
            public class Second:IComparable
            {
                public int i;

                private Second()
                {
                }

                public Second(int x)
                {
                    i = x;
                }

                public override string ToString()
                {
                    return i == ni ? "" : new Minute(i/60).ToString() + ':' + i2(i%60);
                }

                public override bool Equals(object o)
                {
                    if (o == null) return false;
                    if (this.GetType() != o.GetType()) return false;
                    Second s = (Second) o;
                    return i == s.i;
                }

                public override int GetHashCode()
                {
                    return i;
                }

                public int CompareTo(object obj)
                {
                    if (obj == null)
                        return 1;

                    Second other = obj as Second;
                    if (other == null)
                        return 1;

                    return i.CompareTo(other.i);
                }
            }

            [Serializable]
            public class KTimespan:IComparable
            {
                public TimeSpan t;

                private KTimespan()
                {
                }

                public KTimespan(long x)
                {
                    t = new TimeSpan(x == nj ? nj : x/100);
                }

                public override string ToString()
                {
                    return qn(t) ? "" : t.ToString();
                }

                public override bool Equals(object o)
                {
                    if (o == null) return false;
                    if (this.GetType() != o.GetType()) return false;
                    KTimespan n = (KTimespan) o;
                    return t.Ticks == n.t.Ticks;
                }

                public override int GetHashCode()
                {
                    return t.GetHashCode();
                }

                public int CompareTo(object obj)
                {
                    if (obj == null)
                        return 1;

                    KTimespan other = obj as KTimespan;
                    if (other == null)
                        return 1;

                    return t.CompareTo(other.t);
                }
            }

            public class Dict
            {
                public object x;
                public object y;

                public Dict(object X, object Y)
                {
                    x = X;
                    y = Y;
                }
            }

            private static int find(string[] x, string y)
            {
                int i = 0;
                for (; i < x.Length && !x[i].Equals(y);) ++i;
                return i;
            }

            public class Flip
            {
                public string[] x;
                public object[] y;

                public Flip(Dict X)
                {
                    x = (string[]) X.x;
                    y = (object[]) X.y;
                }

                public object at(string s)
                {
                    return y[find(x, s)];
                }
            }

            public static Flip td(object X)
            {
                if (t(X) == 98) return (Flip) X;
                Dict d = (Dict) X;
                Flip a = (Flip) d.x, b = (Flip) d.y;
                int m = c.n(a.x), n = c.n(b.x);
                string[] x = new string[m + n];
                Array.Copy(a.x, 0, x, 0, m);
                Array.Copy(b.x, 0, x, m, n);
                object[] y = new object[m + n];
                Array.Copy(a.y, 0, y, 0, m);
                Array.Copy(b.y, 0, y, m, n);
                return new Flip(new Dict(x, y));
            }

            private static int t(object x)
            {
                if(x == null)
                    return 0;
                return GetIndexForType(x.GetType());
            }

            private static int GetIndexForType(Type type)
            {
                if (type == typeof(Boolean))
                    return -1;
                if (type == typeof(Byte))
                    return -4;
                if (type == typeof(Int16))
                    return -5;
                if (type == typeof(Int32))
                    return -6;
                if (type == typeof(Int64))
                    return -7;
                if (type == typeof(Single))
                    return -8;
                if (type == typeof(Double))
                    return -9;
                if (type == typeof(Char))
                    return -10;
                if (type == typeof(String))
                    return -11;
                if (type == typeof(DateTime))
                    return -12;
                if (type == typeof(Month))
                    return -13;
                if (type == typeof(Date))
                    return -14;
                if (type == typeof(KTimespan))
                    return -16;
                if (type == typeof(Minute))
                    return -17;
                if (type == typeof(Second))
                    return -18;
                if (type == typeof(TimeSpan))
                    return -19;
                if (type == typeof(Boolean[]))
                    return 1;
                if (type == typeof(Byte[]))
                    return 4;
                if (type == typeof(Int16[]))
                    return 5;
                if (type == typeof(Int32[]))
                    return 6;
                if (type == typeof(Int64[]))
                    return 7;
                if (type == typeof(Single[]))
                    return 8;
                if (type == typeof(Double[]))
                    return 9;
                if (type == typeof(Char[]))
                    return 10;
                if (type == typeof(DateTime[]))
                    return 12;
                if (type == typeof(KTimespan[]))
                    return 16;
                if (type == typeof(TimeSpan[]))
                    return 19;
                if (type == typeof(Flip))
                    return 98;
                if (type == typeof(Dict))
                    return 99;
                        
                return 0;
                
            }

            private static int[] nt = {0, 1, 0, 0, 1, 2, 4, 8, 4, 8, 1, 0, 8, 4, 4, 8, 8, 4, 4, 4};
                                 // x.GetType().IsArray

            private static int n(object x)
            {
                return x is Dict
                           ? n(((Dict) x).x)
                           : x is Flip
                                 ? n(((Flip) x).y[0])
                                 : x is char[] ? e.GetBytes((char[]) x).Length : ((Array) x).Length;
            }

            private static int nx(object x)
            {
                int i = 0, n, t = c.t(x), j;
                if (t == 99) return 1 + nx(((Dict) x).x) + nx(((Dict) x).y);
                if (t == 98) return 3 + nx(((Flip) x).x) + nx(((Flip) x).y);
                if (t < 0) return t == -11 ? 2 + ns((string) x) : 1 + nt[-t];
                j = 6;
                n = c.n(x);
                if (t == 0) for (; i < n; ++i) j += nx(((object[]) x)[i]);
                else j += n*nt[t];
                return j;
            }

            public static object at(object x, int i)
            {
                object r = ((Array) x).GetValue(i);
                return qn(r) ? null : r;
            }

            //public static void set(object x,int i,object y){Array.set(x,i,null==y?NU[t(x)]:y);}
            private void w(bool x)
            {
                B[J++] = (byte) (x ? 1 : 0);
            }

            private bool rb()
            {
                return 1 == b[j++];
            }

            private void w(byte x)
            {
                B[J++] = x;
            }

            private byte rx()
            {
                return b[j++];
            }

            private void w(short h)
            {
                B[J++] = (byte) h;
                B[J++] = (byte) (h >> 8);
            }

            private short rh()
            {
                int x = b[j++], y = b[j++];
                return (short) (a ? x & 0xff | y << 8 : x << 8 | y & 0xff);
            }

            private void w(int i)
            {
                w((short) i);
                w((short) (i >> 16));
            }

            private int ri()
            {
                int x = rh(), y = rh();
                return a ? x & 0xffff | y << 16 : x << 16 | y & 0xffff;
            }

            private void w(long j)
            {
                w((int) j);
                w((int) (j >> 32));
            }

            private long rj()
            {
                int x = ri(), y = ri();
                return a ? x & 0xffffffffL | (long) y << 32 : (long) x << 32 | y & 0xffffffffL;
            }

            private void w(float e)
            {
                byte[] b = BitConverter.GetBytes(e);
                foreach (byte i in b) w(i);
            }

            private float re()
            {
                byte c;
                float e;
                if (!a)
                {
                    c = b[j];
                    b[j] = b[j + 3];
                    b[j + 3] = c;
                    c = b[j + 1];
                    b[j + 1] = b[j + 2];
                    b[j + 2] = c;
                }
                e = BitConverter.ToSingle(b, j);
                j += 4;
                return e;
            }

            private void w(double f)
            {
                w(BitConverter.DoubleToInt64Bits(f));
            }

            private double rf()
            {
                return BitConverter.Int64BitsToDouble(rj());
            }

            private void w(char c)
            {
                w((byte) c);
            }

            private char rc()
            {
                return (char) (b[j++] & 0xff);
            }

            private void w(string s)
            {
                byte[] b = e.GetBytes(s);
                foreach (byte i in b) w(i);
                B[J++] = 0;
            }

            private string rs()
            {
                int k = j;
                for (; b[j] != 0; ++j) ;
                string s = e.GetString(b, k, j - k);
                j++;
                return s;
            }

            private void w(Date d)
            {
                w(d.i);
            }

            private Date rd()
            {
                return new Date(ri());
            }

            private void w(Minute u)
            {
                w(u.i);
            }

            private Minute ru()
            {
                return new Minute(ri());
            }

            private void w(Month m)
            {
                w(m.i);
            }

            private Month rm()
            {
                return new Month(ri());
            }

            private void w(Second v)
            {
                w(v.i);
            }

            private Second rv()
            {
                return new Second(ri());
            }

            private void w(TimeSpan t)
            {
                w(qn(t) ? ni : (int) (t.Ticks/10000));
            }

            private TimeSpan rt()
            {
                int i = ri();
                return new TimeSpan(qn(i) ? ni : 10000L*i);
            }

            private void w(DateTime p)
            {
                w(qn(p) ? nj : (100*(p.Ticks - o)));
            }

            private DateTime rz()
            {
                double f = rf();
                return Double.IsInfinity(f)
                           ? (f < 0 ? za : zw)
                           : new DateTime(qn(f) ? 0 : 10000*(long) Math.Round(8.64e7*f) + o);
            }

            private void w(KTimespan t)
            {
                w(qn(t) ? nj : (t.t.Ticks*100));
            }

            private KTimespan rn()
            {
                return new KTimespan(rj());
            }

            private DateTime rp()
            {
                long j = rj(), d = j < 0 ? (j + 1)/100 - 1 : j/100;
                DateTime p = new DateTime(j == nj ? 0 : o + d);
                return p;
            }

            private void w(object x)
            {
                int t = c.t(x);
                w((byte) t);
                if (t < 0)
                    switch (t)
                    {
                        case -1:
                            w((bool) x);
                            return;
                        case -4:
                            w((byte) x);
                            return;
                        case -5:
                            w((short) x);
                            return;
                        case -6:
                            w((int) x);
                            return;
                        case -7:
                            w((long) x);
                            return;
                        case -8:
                            w((float) x);
                            return;
                        case -9:
                            w((double) x);
                            return;
                        case -10:
                            w((char) x);
                            return;
                        case -11:
                            w((string) x);
                            return;
                        case -12:
                            w((DateTime) x);
                            return;
                        case -13:
                            w((Month) x);
                            return;
                        case -14:
                            w((Date) x);
                            return;
                        case -15:
                            w((DateTime) x);
                            return;
                        case -16:
                            w((KTimespan) x);
                            return;
                        case -17:
                            w((Minute) x);
                            return;
                        case -18:
                            w((Second) x);
                            return;
                        case -19:
                            w((TimeSpan) x);
                            return;
                    }
                if (t == 99)
                {
                    Dict r = (Dict) x;
                    w(r.x);
                    w(r.y);
                    return;
                }
                B[J++] = 0;
                if (t == 98)
                {
                    Flip r = (Flip) x;
                    B[J++] = 99;
                    w(r.x);
                    w(r.y);
                    return;
                }
                w(c.n(x));
                switch (t)
                {
                    case 0:
                        foreach (object o in (object[]) x) w(o);
                        return;
                    case 1:
                        foreach (bool o in (bool[]) x) w(o);
                        return;
                    case 4:
                        foreach (byte o in (byte[]) x) w(o);
                        return;
                    case 5:
                        foreach (short o in (short[]) x) w(o);
                        return;
                    case 6:
                        foreach (int o in (int[]) x) w(o);
                        return;
                    case 7:
                        foreach (long o in (long[]) x) w(o);
                        return;
                    case 8:
                        foreach (float o in (float[]) x) w(o);
                        return;
                    case 9:
                        foreach (double o in (double[]) x) w(o);
                        return;
                    case 10:
                        foreach (byte b in e.GetBytes((char[]) x)) w(b);
                        return;
                    case 11:
                        foreach (string o in (string[]) x) w(o);
                        return;
                    case 12:
                        foreach (DateTime o in (DateTime[]) x) w(o);
                        return;
                    case 13:
                        foreach (Month o in (Month[]) x) w(o);
                        return;
                    case 14:
                        foreach (Date o in (Date[]) x) w(o);
                        return;
                    case 15:
                        foreach (DateTime o in (DateTime[]) x) w(o);
                        return;
                    case 16:
                        foreach (KTimespan o in (KTimespan[]) x) w(o);
                        return;
                    case 17:
                        foreach (Minute o in (Minute[]) x) w(o);
                        return;
                    case 18:
                        foreach (Second o in (Second[]) x) w(o);
                        return;
                    case 19:
                        foreach (TimeSpan o in (TimeSpan[]) x) w(o);
                        return;
                }
            }

            private object r()
            {
                int i = 0, n, t = (sbyte) b[j++];
                if (t < 0)
                    switch (t)
                    {
                        case -1:
                            return rb();
                        case -4:
                            return b[j++];
                        case -5:
                            return rh();
                        case -6:
                            return ri();
                        case -7:
                            return rj();
                        case -8:
                            return re();
                        case -9:
                            return rf();
                        case -10:
                            return rc();
                        case -11:
                            return rs();
                        case -12:
                            return rp();
                        case -13:
                            return rm();
                        case -14:
                            return rd();
                        case -15:
                            return rz();
                        case -16:
                            return rn();
                        case -17:
                            return ru();
                        case -18:
                            return rv();
                        case -19:
                            return rt();
                    }
                if (t > 99)
                {
                    if (t == 101 && b[j++] == 0) return null;
                    throw new KException("func");
                }
                if (t == 99) return new Dict(r(), r());
                j++;
                if (t == 98) return new Flip((Dict) r());
                n = ri();
                switch (t)
                {
                    case 0:
                        object[] L = new object[n];
                        for (; i < n; i++) L[i] = r();
                        return L;
                    case 1:
                        bool[] B = new bool[n];
                        for (; i < n; i++) B[i] = rb();
                        return B;
                    case 4:
                        byte[] G = new byte[n];
                        for (; i < n; i++) G[i] = b[j++];
                        return G;
                    case 5:
                        short[] H = new short[n];
                        for (; i < n; i++) H[i] = rh();
                        return H;
                    case 6:
                        int[] I = new int[n];
                        for (; i < n; i++) I[i] = ri();
                        return I;
                    case 7:
                        long[] J = new long[n];
                        for (; i < n; i++) J[i] = rj();
                        return J;
                    case 8:
                        float[] E = new float[n];
                        for (; i < n; i++) E[i] = re();
                        return E;
                    case 9:
                        double[] F = new double[n];
                        for (; i < n; i++) F[i] = rf();
                        return F;
                    case 10:
                        char[] C = e.GetChars(b, j, n);
                        j += n;
                        return C;
                    case 11:
                        String[] S = new String[n];
                        for (; i < n; i++) S[i] = rs();
                        return S;
                    case 12:
                        DateTime[] P = new DateTime[n];
                        for (; i < n; i++) P[i] = rp();
                        return P;
                    case 13:
                        Month[] M = new Month[n];
                        for (; i < n; i++) M[i] = rm();
                        return M;
                    case 14:
                        Date[] D = new Date[n];
                        for (; i < n; i++) D[i] = rd();
                        return D;
                    case 15:
                        DateTime[] Z = new DateTime[n];
                        for (; i < n; i++) Z[i] = rz();
                        return Z;
                    case 16:
                        KTimespan[] N = new KTimespan[n];
                        for (; i < n; i++) N[i] = rn();
                        return N;
                    case 17:
                        Minute[] U = new Minute[n];
                        for (; i < n; i++) U[i] = ru();
                        return U;
                    case 18:
                        Second[] V = new Second[n];
                        for (; i < n; i++) V[i] = rv();
                        return V;
                    case 19:
                        TimeSpan[] T = new TimeSpan[n];
                        for (; i < n; i++) T[i] = rt();
                        return T;
                }
                return null;
            }

            private void w(int i, object x)
            {
                int n = nx(x) + 8;
                B = new byte[n];
                B[0] = 1;
                B[1] = (byte) i;
                J = 4;
                w(n);
                w(x);
                s.Write(B, 0, n);
            }

            private void read(byte[] b)
            {
                int i = 0, j, n = b.Length;
                for (; i < n; i += j) if (0 == (j = s.Read(b, i, Math.Min(_maxBufferSize, n - i)))) throw new Exception("read");
            }

            public object k()
            {
                read(b = new byte[8]);
                a = b[0] == 1;
                bool c = b[2] == 1;
                j = 4;
                read(b = new byte[ri() - 8]);
                if (c) u();
                else j = 0;
                if (b[0] == 128)
                {
                    j = 1;
                    throw new KException(rs());
                }
                return r();
            }

            public object k(object x)
            {
                w(1, x);
                return k();
            }

            public object k(string s)
            {
                return k(cs(s));
            }

            private char[] cs(string s)
            {
                return s.ToCharArray();
            }

            public object k(string s, object x)
            {
                object[] a = {cs(s), x};
                return k(a);
            }

            public object k(string s, object x, object y)
            {
                object[] a = {cs(s), x, y};
                return k(a);
            }

            public object k(string s, object x, object y, object z)
            {
                object[] a = {cs(s), x, y, z};
                return k(a);
            }

            public void ks(String s)
            {
                w(0, cs(s));
            }

            public void ks(String s, Object x)
            {
                Object[] a = {cs(s), x};
                w(0, a);
            }

            public void ks(String s, Object x, Object y)
            {
                Object[] a = {cs(s), x, y};
                w(0, a);
            }
        }

        public class KException : Exception
        {
            public KException()
            {
            }

            public KException(string message) : base(message)
            {
            }
        }
    }
