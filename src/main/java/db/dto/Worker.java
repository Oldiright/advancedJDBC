package db.dto;


public class Worker {

        private String name;
        private String birthday;
        private String level;
        private  int salary;

        @Override
        public String toString() {
                return "{" +
                        "name='" + name + '\'' +
                        ", birthday='" + birthday + '\'' +
                        ", level='" + level + '\'' +
                        ", salary=" + salary +
                        '}';
        }

        public void setName(String name) {
                this.name = name;
        }

        public void setBirthday(String birthday) {
                this.birthday = birthday;
        }

        public void setLevel(String level) {
                this.level = level;
        }

        public void setSalary(int salary) {
                this.salary = salary;
        }

        public String getName() {
                return name;
        }

        public String getBirthday() {
                return birthday;
        }

        public String getLevel() {
                return level;
        }

        public int getSalary() {
                return salary;
        }
}
