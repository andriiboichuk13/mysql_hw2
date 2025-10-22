import pymysql
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Database connection
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='MySQL_Student123',  # Add your password
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)


def create_database():
    """Create database and tables"""
    with connection.cursor() as cursor:
        # Create database
        cursor.execute("DROP DATABASE IF EXISTS blog_db5")
        cursor.execute("CREATE DATABASE blog_db5")
        cursor.execute("USE blog_db5")

        # Create users table
        cursor.execute("""
                       CREATE TABLE users
                       (
                           user_id    INT PRIMARY KEY AUTO_INCREMENT,
                           username   VARCHAR(255),
                           email      VARCHAR(255),
                           country    VARCHAR(255),
                           created_at DATE,
                           status     VARCHAR(255)
                       ) ENGINE=InnoDB
                       """)

        # Create posts table
        cursor.execute("""
                       CREATE TABLE posts
                       (
                           post_id    INT PRIMARY KEY AUTO_INCREMENT,
                           user_id    INT,
                           title      VARCHAR(200),
                           category   VARCHAR(255),
                           created_at DATE,
                           views      INT,
                           likes      INT
                       ) ENGINE=InnoDB
                       """)

        # Create comments table
        cursor.execute("""
                       CREATE TABLE comments
                       (
                           comment_id   INT PRIMARY KEY AUTO_INCREMENT,
                           post_id      INT,
                           user_id      INT,
                           comment_text TEXT,
                           created_at   DATE,
                           likes        INT
                       ) ENGINE=InnoDB
                       """)

    connection.commit()
    print("✅ Database and tables created")


def insert_users(num_users=100000):
    """Insert users into database"""
    print(f"📝 Inserting {num_users:,} users...")

    with connection.cursor() as cursor:
        cursor.execute("USE blog_db5")

        batch_size = 10000
        statuses = ['active', 'inactive', 'suspended']

        for i in range(0, num_users, batch_size):
            users_data = []
            current_batch = min(batch_size, num_users - i)

            for _ in range(current_batch):
                user = (
                    fake.user_name()[:50],
                    fake.email(),
                    fake.country(),
                    fake.date_between(start_date='-3y', end_date='today'),
                    random.choice(statuses)
                )
                users_data.append(user)

            cursor.executemany(
                "INSERT INTO users (username, email, country, created_at, status) VALUES (%s, %s, %s, %s, %s)",
                users_data
            )
            connection.commit()

            if (i + batch_size) % 50000 == 0:
                print(f"  ↳ Inserted {i + batch_size:,} users...")

    print(f"✅ {num_users:,} users inserted")


def insert_posts(num_posts=120000):
    """Insert posts into database"""
    print(f"📝 Inserting {num_posts:,} posts...")

    with connection.cursor() as cursor:
        cursor.execute("USE blog_db5")

        # Get total users count
        cursor.execute("SELECT COUNT(*) as count FROM users")
        total_users = cursor.fetchone()['count']

        batch_size = 10000
        categories = ['Technology', 'Travel', 'Food', 'Fashion', 'Sports', 'Music']

        for i in range(0, num_posts, batch_size):
            posts_data = []
            current_batch = min(batch_size, num_posts - i)

            for k in range(current_batch):
                post = (
                    random.randint(1, total_users),
                    fake.sentence(nb_words=6)[:200],
                    random.choice(categories),
                    fake.date_between(start_date='-2y', end_date='today'),
                    random.randint(0, 10000),
                    random.randint(0, 1000)
                )
                posts_data.append(post)

            cursor.executemany(
                "INSERT INTO posts (user_id, title, category, created_at, views, likes) VALUES (%s, %s, %s, %s, %s, %s)",
                posts_data
            )
            connection.commit()

            if (i + batch_size) % 50000 == 0:
                print(f"  ↳ Inserted {i + batch_size:,} posts...")

    print(f"✅ {num_posts:,} posts inserted")


def insert_comments(num_comments=200000):
    """Insert comments into database"""
    print(f"📝 Inserting {num_comments:,} comments...")

    with connection.cursor() as cursor:
        cursor.execute("USE blog_db5")

        # Get total counts
        cursor.execute("SELECT COUNT(*) as count FROM users")
        total_users = cursor.fetchone()['count']

        cursor.execute("SELECT COUNT(*) as count FROM posts")
        total_posts = cursor.fetchone()['count']

        batch_size = 10000

        for i in range(0, num_comments, batch_size):
            comments_data = []
            current_batch = min(batch_size, num_comments - i)

            for k in range(current_batch):
                comment = (
                    random.randint(1, total_posts),
                    random.randint(1, total_users),
                    fake.text(max_nb_chars=200),
                    fake.date_between(start_date='-2y', end_date='today'),
                    random.randint(0, 100)
                )
                comments_data.append(comment)

            cursor.executemany(
                "INSERT INTO comments (post_id, user_id, comment_text, created_at, likes) VALUES (%s, %s, %s, %s, %s)",
                comments_data
            )
            connection.commit()

            if (i + batch_size) % 50000 == 0:
                print(f"  ↳ Inserted {i + batch_size:,} comments...")

    print(f"✅ {num_comments:,} comments inserted")


def show_statistics():
    """Display table statistics"""
    print("\n" + "=" * 60)
    print("📊 TABLE STATISTICS")
    print("=" * 60)

    with connection.cursor() as cursor:
        cursor.execute("USE blog_db4")

        tables = ['users', 'posts', 'comments']
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            print(f"  {table.upper():15s}: {count:>12,} rows")

    print("=" * 60)


def main():
    """Main execution function"""
    print("=" * 60)
    print("🚀 BLOG DATABASE GENERATOR")
    print("=" * 60)
    print("Target: 1M users, 1.2M posts, 2M comments")
    print("This will take approximately 20-30 minutes...")
    print("=" * 60 + "\n")

    try:
        # Create database and tables
        create_database()

        # Insert data
        insert_users(2000)  # 1 million users
        insert_posts(2500)  # 1.2 million posts
        insert_comments(3000)  # 2 million comments

        # Show statistics
        show_statistics()

        print("\n✅ Data generation complete!")
        print("\n⚠️  Next steps:")
        print("  1. Run the non-optimized query (it will be SLOW)")
        print("  2. Create indexes from query_optimization.sql")
        print("  3. Run the optimized query (it will be FAST)")
        print("  4. Compare with EXPLAIN and EXPLAIN ANALYZE")

    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
    print("\n⚠️  Required packages: pip install pymysql faker")