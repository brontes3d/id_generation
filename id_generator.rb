class IdGenerator < ActiveRecord::Base

  #TODO: bug still exists when a generator is created and then used without a COMMIT of transaction between

  def self.sequential_generator_for(purpose)
    @@sequential_generators ||= {}
    @@sequential_generators[purpose] ||= IdGenerator.find_by_purpose(purpose)    
    if(@@sequential_generators[purpose].nil?)
      generator = IdGenerator.new
      IdGenerator.transaction_in_seperate_connection(generator) do
        generator.purpose = purpose
        generator.style = "sequential_numbers"
        generator.number_adder = 100001
        generator.save!
      end
      @@sequential_generators[purpose] = generator
    end
    return @@sequential_generators[purpose]
  end
    
  def self.pseudo_random_generator_for(purpose)
    @@pseudo_random_generators ||= {}
    @@pseudo_random_generators[purpose] ||= IdGenerator.find_by_purpose(purpose)    
    if(@@pseudo_random_generators[purpose].nil?)
      rand_generator = IdGenerator.new
      IdGenerator.transaction_in_seperate_connection(rand_generator) do
        rand_generator.purpose = purpose
        rand_generator.style = "pseudo_random"
        rand_generator.letter_char_count = 3
        rand_generator.number_char_count = 6
        rand_generator.seed_position_in_numbers = 5 #meaning at index 6, the end of the string
        rand_generator.universal_id_prefix = "ZZ"
        rand_generator.force_letter_prefix = ""
        rand_generator.letter_alphabet_yaml = ['B','C','D','F','G','H','J','K','M','N','P','R','S','T','V','W','Z'].to_yaml
        rand_generator.number_alphabet_yaml = [0,1,2,3,4,5,6,7,8,9].to_yaml      
        rand_generator.number_adder = 657281
        rand_generator.number_prime = 365473      
        rand_generator.letter_adder = 2456
        rand_generator.letter_prime = 9851
        rand_generator.save!
      end
      @@pseudo_random_generators[purpose] = rand_generator
    end
    return @@pseudo_random_generators[purpose]
  end  
  
  def self.transaction_in_seperate_connection(generator = nil)
    @@transactionsemaphore ||= Mutex.new
    @@transactionsemaphore.synchronize do
      IdGenerator.connection_pool.with_connection do |conn|
          conn.transaction do
            yield conn
          end
      end
    end
  end
    
  def next_id
    seed_to_use = nil
    
    IdGenerator.transaction_in_seperate_connection do |conn|
        #Without wrapping these in a transaction, we WILL have a chance for collisions
        #TODO: test this with NDB-cluster!
          conn.execute("Update id_generators set last_seed = last_seed + 1 WHERE id = #{self.id}")
          value_selected = conn.uncached {
            conn.select_value("Select last_seed from id_generators WHERE id = #{self.id}")
          }
          unless value_selected
            raise ArgumentError, "ERROR: Somebody deleted me from the database, but I'm still cached in rails: #{self.inspect}"
          end
          seed_to_use = value_selected.to_i
          # puts "#{purpose} value_selected: #{value_selected} seed_to_use: #{seed_to_use}"        
    end
    #the obvious implementation that created collisions was:
      # self.last_seed += 1
      # save!
      # seed_to_use = self.last_seed

    return generate_id(seed_to_use)
  end
    
  def letter_alphabet
    @letter_alphabet ||= YAML::load(letter_alphabet_yaml)
  end
  
  def number_alphabet
    @number_alphabet ||= YAML::load(number_alphabet_yaml)
  end
  
  def number_size
    if seed_position_in_numbers.nil?
      @@number_size ||= number_char_count
    else
      @@number_size ||= number_char_count - 1
    end
  end
  
  def letter_size
    @letter_size ||= letter_char_count - force_letter_prefix.size
  end
  
  def number_range
    @number_range ||= number_alphabet.size ** number_size
  end
  
  def letter_range
    @letter_range ||= letter_alphabet.size ** letter_size
  end
      
  def generate_id(seed)
    if self.style == 'pseudo_random'
      raise "seed too large" if(seed > possible_range_of_seeds)
      letter_seed = seed / number_range
      number_seed = seed % number_range
      # puts "using letter_seed #{letter_seed} number_seed #{number_seed}"
      number_portion = generate_numeric_portion(number_seed)
      check_digit = calc_check_digit(number_portion)
      letter_portion = generate_alpha_portion(letter_seed)
      to_return = universal_id_prefix
      to_return += force_letter_prefix
      to_return + "#{letter_portion}#{number_portion}#{check_digit}"      
    elsif self.style == 'sequential_numbers'
      return seed + number_adder
    else
      raise "unknown generator style: '#{self.style.to_s}'"
    end    
  end

  def possible_range_of_seeds
    number_range * letter_range
  end

  def generate_alpha_portion(seed)
    number_to_letters(((seed * letter_prime) + letter_adder) % letter_range)
  end

  def generate_numeric_portion(seed)
    format("%0#{number_size}d", ((seed * number_prime) + number_adder) % number_range)
  end

#helpers

  def calc_check_digit(number)
    digits = number.to_s
    sum = 0;
    (0...digits.size).each do |index|
      digit = digits[index,1].to_i
      digit = ((digit*2) % 9) if(index % 2 == 0)
      sum += digit
    end
    sum % 10;
  end

  def number_to_letters(number)
    to_return = ""
    letter_size.times do |times_to_divide|
      index = number
      times_to_divide.times do
        index = index/ letter_alphabet.size
      end
      to_return += letter_alphabet[index % letter_alphabet.size]
    end
    # puts "converted number #{number} to letters #{to_return}"
    to_return
  end

  def letters_to_number(letters)
    number = 0
    letters.size.times do |index|
      number += letter_alphabet.index(letters[index,1]) * (letter_alphabet.size ** index)
    end
    # puts "converted letters #{letters} to number #{number}"
    number
  end
  
end
