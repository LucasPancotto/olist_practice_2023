from unicodedata import numeric
import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        orders = self.data['orders']

        print('drop null deliveries')
        orders = orders.dropna(subset= ['order_delivered_customer_date'])

        print('changing data type to date time')

        orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
        orders['order_approved_at']= pd.to_datetime(orders['order_approved_at'])
        orders['order_delivered_carrier_date']= pd.to_datetime(orders['order_delivered_carrier_date'])
        orders['order_delivered_customer_date']= pd.to_datetime(orders['order_delivered_customer_date'])
        orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])
        print('defining new columns')

        import datetime
        one_day_delta = datetime.timedelta(days=1) # a "timedelta" object of 1 day

        #compute waittime  (order_delivered_customer_date     order_purchase_timestamp         )
        orders['wait_time'] = (orders.order_delivered_customer_date  - orders.order_purchase_timestamp)/one_day_delta
        #compute expected waittime
        orders['expected_wait_time'] = (orders.order_estimated_delivery_date    - orders.order_purchase_timestamp)/one_day_delta
        #delay vs expected

        orders['delay_vs_expected'] = (orders.order_delivered_customer_date - orders.order_estimated_delivery_date)/one_day_delta
        #si <0 entonces no hay delay

        def handle_delay(x):
            if x<=0:
                return 0
            else:
                return x

        orders['delay_vs_expected'] = orders['delay_vs_expected'].apply(handle_delay)
        wait_time_df = orders[['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected']]


        return wait_time_df





    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        print('getting data')
        reviews = self.data['order_reviews']

        print('defining score functions')
        def dim_five_star(d):
            def find_five(n):
                if n == 5:
                    return 1
                return 0
            return d['review_score'].apply(find_five)





        def dim_one_star(d):
            def find_one(n):
                if n == 1:
                    return 1
                return 0
            return d['review_score'].apply(find_one)

        print('concatenating dataframe')
        return pd.concat( [reviews.order_id , dim_five_star(reviews) , dim_one_star(reviews) ,reviews.review_score ] , axis = 1 , keys = ['order_id', 'dim_five_star', 'dim_one_star', 'review_score'])

    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        print('getting data')
        products = self.data['products']
        order_items = self.data['order_items']
        orders = self.data['orders']

        print('merging data')
        order_and_items = order_items.merge(orders , on = 'order_id' , how = 'inner')
        products_and_orders = order_and_items.merge(products , on = 'product_id' , how = 'inner')

        print('grouping by order id and counting')
        df = products_and_orders.groupby('order_id').count().sort_values('product_id', ascending = False)['product_id']
        print('making the dataframe')
        number_of_products = pd.DataFrame(df)
        return number_of_products.reset_index()


    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        print('getting data')
        order_items = self.data['order_items']
        orders = self.data['orders']
        sellers = self.data['sellers']

        print('merging data')
        order_and_items = order_items.merge(orders , on = 'order_id' , how = 'inner')
        sellers_and_orders = order_and_items.merge(sellers , on = 'seller_id' , how = 'inner')

        print('counting sellers')
        serie = sellers_and_orders.groupby('order_id').nunique().sort_values('seller_id')['seller_id']
        print('defining dataframe')
        number_of_unique_sellers = pd.DataFrame(serie).reset_index()

        return number_of_unique_sellers


    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """


        print('getting data')
        order_items = self.data['order_items']
        orders = self.data['orders']


        print('merging data')

        order_and_items = order_items.merge(orders , on = 'order_id' , how = 'inner')


        print('define dataframe')
        price_and_freight = pd.DataFrame(order_and_items.groupby('order_id').sum()[['price' , 'freight_value']]).reset_index()
        return price_and_freight


    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """

        # $CHALLENGIFY_BEGIN

        # import data
        data = self.data
        orders = data['orders']
        order_items = data['order_items']
        sellers = data['sellers']
        customers = data['customers']

        # Since one zip code can map to multiple (lat, lng), take the first one
        geo = data['geolocation']
        geo = geo.groupby('geolocation_zip_code_prefix',
                          as_index=False).first()

        # Merge geo_location for sellers
        sellers_mask_columns = [
            'seller_id', 'seller_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'
        ]

        sellers_geo = sellers.merge(
            geo,
            how='left',
            left_on='seller_zip_code_prefix',
            right_on='geolocation_zip_code_prefix')[sellers_mask_columns]

        # Merge geo_location for customers
        customers_mask_columns = ['customer_id', 'customer_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']

        customers_geo = customers.merge(
            geo,
            how='left',
            left_on='customer_zip_code_prefix',
            right_on='geolocation_zip_code_prefix')[customers_mask_columns]

        # Match customers with sellers in one table
        customers_sellers = customers.merge(orders, on='customer_id')\
            .merge(order_items, on='order_id')\
            .merge(sellers, on='seller_id')\
            [['order_id', 'customer_id','customer_zip_code_prefix', 'seller_id', 'seller_zip_code_prefix']]

        # Add the geoloc
        matching_geo = customers_sellers.merge(sellers_geo,
                                            on='seller_id')\
            .merge(customers_geo,
                   on='customer_id',
                   suffixes=('_seller',
                             '_customer'))
        # Remove na()
        matching_geo = matching_geo.dropna()

        matching_geo.loc[:, 'distance_seller_customer'] =\
            matching_geo.apply(lambda row:
                               haversine_distance(row['geolocation_lng_seller'],
                                                  row['geolocation_lat_seller'],
                                                  row['geolocation_lng_customer'],
                                                  row['geolocation_lat_customer']),
                               axis=1)
        # Since an order can have multiple sellers,
        # return the average of the distance per order
        order_distance =\
            matching_geo.groupby('order_id',
                                 as_index=False).agg({'distance_seller_customer':
                                                      'mean'})

        return order_distance


    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        # Hint: make sure to re-use your instance methods defined above
        training_set =\
            self.get_wait_time(is_delivered)\
                .merge(
                self.get_review_score(), on='order_id'
            ).merge(
                self.get_number_products(), on='order_id'
            ).merge(
                self.get_number_sellers(), on='order_id'
            ).merge(
                self.get_price_and_freight(), on='order_id'
            )
        # Skip heavy computation of distance_seller_customer unless specified
        if with_distance_seller_customer:
            training_set = training_set.merge(
                self.get_distance_seller_customer(), on='order_id')

        return training_set.dropna()
