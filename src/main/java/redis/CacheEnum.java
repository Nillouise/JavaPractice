package redis;

/**
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public class CacheEnum {
    public enum ErrorCode{
        REDIS_ERROR(1,"error occur in redis"),
        NO_EXIST_REDIS(2,"didn't exist in redis"),
        ILLEGAL_PARAMETER(3,"illegal parameter"),
        OTHER_ERROR(4,"some error occur"),
        LOCAL_CACHE_ERROR(5,"local server internal error occur"),
        IO_EXCEPTION(6,"searialization io exception"),
        ;
        //考虑网络传输，用int类型的type比较好吧
        private int type;
        private String description;

        ErrorCode(int type, String description) {
            this.type = type;
            this.description = description;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }
    public enum Source{
        LOCAL_CACHE(0,"from local CACHE"),
        REDIS(1,"from redis")
        ;
        //考虑网络传输，用int类型的type比较好吧
        private int type;
        private String description;

        Source(int type, String description) {
            this.type = type;
            this.description = description;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }
}
